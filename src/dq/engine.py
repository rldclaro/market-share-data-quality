"""Motor de Data Quality.

Quatro operações cobrem todos os tratamentos do case, e TODAS registram no log:

  check()      -> marca a falha no registro (dq_flags); não altera o dado
  correct()    -> aplica correção determinística; guarda o original em <coluna>_raw
                  e grava a trilha antes/depois em ms_dq.corrections
  quarantine() -> retira o registro do fluxo e o grava em ms_dq.quarantine com regra,
                  severidade, motivo, arquivo de origem e payload original (JSON)
  record()     -> regra de nível de dataset (reconciliação, contagens)

No fim, flush() grava log, quarentena e correções nas tabelas do schema ms_dq.

Compatível com serverless (Spark Connect): não usa cache(), sparkContext nem RDD.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .models import CatalogoRegras, Classificacao, Regra

FLAGS = "dq_flags"     # array com os ids das regras violadas pelo registro
STATUS = "dq_status"   # classificação final do registro
_FAIL = "__dq_fail"    # coluna temporária com o resultado da regra


@dataclass
class ResultadoRegra:
    regra: Regra
    alvo: str              # tabela/dataset avaliado
    avaliados: int
    falhas: int
    valor_impactado: float | None = None
    detalhes: str = ""

    @property
    def resultado(self) -> Classificacao:
        return Classificacao.APROVADO if self.falhas == 0 else self.regra.se_falhar


def _seguro(cond: Column) -> Column:
    """NULL vira False: uma comparação com nulo não pode contar como falha nem como aprovação silenciosa."""
    return F.coalesce(cond, F.lit(False))


def _com_flags(df: DataFrame) -> DataFrame:
    if FLAGS in df.columns:
        return df
    return df.withColumn(FLAGS, F.array().cast(T.ArrayType(T.StringType())))


def _adiciona_flag(regra_id: str, cond: Column) -> Column:
    return F.when(cond, F.array_union(F.col(FLAGS), F.array(F.lit(regra_id)))).otherwise(F.col(FLAGS))


class DQEngine:
    def __init__(self, spark: SparkSession, catalogo: CatalogoRegras, run_id: str):
        self.spark = spark
        self.catalogo = catalogo
        self.run_id = run_id
        self._resultados: list[ResultadoRegra] = []
        self._quarentena: list[DataFrame] = []
        self._correcoes: list[DataFrame] = []

    # ------------------------------------------------------------------ estatísticas
    def _estatisticas(self, df: DataFrame, avaliado: Column, col_valor: str | None):
        """Uma única passada no dado: avaliados, falhas e valor impactado (R$)."""
        aggs = [F.sum(avaliado.cast("long")).alias("av"), F.sum(F.col(_FAIL).cast("long")).alias("fl")]
        tem_valor = bool(col_valor) and col_valor in df.columns
        if tem_valor:
            aggs.append(F.sum(F.when(F.col(_FAIL), F.abs(F.col(col_valor).cast("double")))).alias("vl"))
        row = df.agg(*aggs).first()
        valor = float(row["vl"]) if tem_valor and row["vl"] is not None else None
        return int(row["av"] or 0), int(row["fl"] or 0), valor

    def _prepara(self, df: DataFrame, falha: Column, avaliado: Column | None):
        avaliado = _seguro(avaliado) if avaliado is not None else F.lit(True)
        return _com_flags(df).withColumn(_FAIL, avaliado & _seguro(falha)), avaliado

    # ------------------------------------------------------------------ operações
    def record(self, regra_id: str, alvo: str, avaliados: int, falhas: int,
               detalhes: str = "", valor_impactado: float | None = None) -> ResultadoRegra:
        """Regra de dataset: o chamador informa os números."""
        res = ResultadoRegra(self.catalogo[regra_id], alvo, int(avaliados), int(falhas), valor_impactado, detalhes)
        self._resultados.append(res)
        return res

    def check(self, df: DataFrame, regra_id: str, falha: Column, *, alvo: str,
              avaliado: Column | None = None, col_valor: str | None = "sales_value_brl",
              detalhes: str = "") -> DataFrame:
        """Marca a falha em dq_flags. O registro continua no fluxo."""
        df, av = self._prepara(df, falha, avaliado)
        n_av, n_fl, vl = self._estatisticas(df, av, col_valor)
        self.record(regra_id, alvo, n_av, n_fl, detalhes, vl)
        return df.withColumn(FLAGS, _adiciona_flag(regra_id, F.col(_FAIL))).drop(_FAIL)

    def correct(self, df: DataFrame, regra_id: str, falha: Column, correcoes: dict[str, Column], *,
                alvo: str, chave: list[str], avaliado: Column | None = None,
                col_valor: str | None = "sales_value_brl", detalhes: str = "") -> DataFrame:
        """Aplica a correção só onde a regra falha.

        - o valor original fica em <coluna>_raw (na primeira correção da coluna);
        - cada alteração gera uma linha de auditoria (antes -> depois);
        - a condição é calculada ANTES de alterar o dado, para a correção não mudar o próprio resultado.
        """
        df, av = self._prepara(df, falha, avaliado)
        n_av, n_fl, vl = self._estatisticas(df, av, col_valor)
        self.record(regra_id, alvo, n_av, n_fl, detalhes, vl)
        if n_fl == 0:
            return df.drop(_FAIL)

        chave_json = F.to_json(F.struct(*[F.col(c) for c in chave]))
        for col, expr in correcoes.items():
            self._correcoes.append(
                df.where(F.col(_FAIL)).select(
                    F.lit(self.run_id).alias("run_id"), F.lit(regra_id).alias("regra_id"),
                    F.lit(alvo).alias("alvo"), chave_json.alias("chave"), F.lit(col).alias("coluna"),
                    F.col(col).cast("string").alias("valor_antes"), expr.cast("string").alias("valor_depois"),
                )
            )
        for col in correcoes:
            if f"{col}_raw" not in df.columns:
                df = df.withColumn(f"{col}_raw", F.col(col))
        projecao = [
            F.when(F.col(_FAIL), correcoes[c]).otherwise(F.col(c)).alias(c) if c in correcoes else F.col(c)
            for c in df.columns if c != FLAGS
        ]
        return df.select(*projecao, _adiciona_flag(regra_id, F.col(_FAIL)).alias(FLAGS)).drop(_FAIL)

    def quarantine(self, df: DataFrame, regra_id: str, falha: Column, *, alvo: str, chave: list[str],
                   motivo: Column | str | None = None, avaliado: Column | None = None,
                   col_valor: str | None = "sales_value_brl", detalhes: str = "") -> DataFrame:
        """Retira do fluxo os registros que falham e guarda o payload original para investigação."""
        regra = self.catalogo[regra_id]
        df, av = self._prepara(df, falha, avaliado)
        n_av, n_fl, vl = self._estatisticas(df, av, col_valor)
        self.record(regra_id, alvo, n_av, n_fl, detalhes, vl)
        if n_fl == 0:
            return df.drop(_FAIL)

        colunas_payload = [c for c in df.columns if c != _FAIL]
        motivo_col = motivo if isinstance(motivo, Column) else F.lit(motivo or regra.descricao)
        tem_valor = bool(col_valor) and col_valor in df.columns
        self._quarentena.append(
            df.where(F.col(_FAIL)).select(
                F.lit(self.run_id).alias("run_id"),
                F.lit(regra.se_falhar.value).alias("classificacao"),
                F.lit(regra_id).alias("regra_id"),
                F.lit(regra.severidade.value).alias("severidade"),
                F.lit(regra.dimensao.value).alias("dimensao"),
                F.lit(alvo).alias("alvo"),
                (F.col("_source_file") if "_source_file" in df.columns else F.lit(None)).cast("string").alias("arquivo_origem"),
                F.to_json(F.struct(*[F.col(c) for c in chave])).alias("chave"),
                motivo_col.cast("string").alias("motivo"),
                (F.col(col_valor) if tem_valor else F.lit(None)).cast("double").alias("valor_brl"),
                F.to_json(F.struct(*[F.col(c) for c in colunas_payload])).alias("payload"),
            )
        )
        return df.where(~F.col(_FAIL)).drop(_FAIL)

    # ------------------------------------------------------------------ status final
    def com_status(self, df: DataFrame) -> DataFrame:
        """dq_status = classificação de maior precedência entre as regras violadas (padrão: APROVADO)."""
        df = _com_flags(df)
        pares = []
        for r in self.catalogo:
            pares += [F.lit(r.id), F.lit(r.se_falhar.rank)]
        rank_por_regra = F.create_map(*pares)
        rank = F.coalesce(F.array_max(F.transform(F.col(FLAGS), lambda x: rank_por_regra[x])), F.lit(0))
        rotulos = F.array(*[F.lit(c.value) for c in Classificacao])
        return df.withColumn(STATUS, F.element_at(rotulos, (rank + 1).cast("int")))

    # ------------------------------------------------------------------ persistência
    def resultados_df(self) -> DataFrame:
        linhas = [
            (self.run_id, r.regra.id, r.regra.descricao, r.regra.dimensao.value, r.alvo, r.regra.escopo,
             r.regra.severidade.value, r.regra.criterio, r.regra.tratamento, r.avaliados, r.falhas,
             round(r.falhas / r.avaliados, 6) if r.avaliados else 0.0,
             round(r.valor_impactado, 2) if r.valor_impactado is not None else None,
             r.resultado.value, r.detalhes)
            for r in self._resultados
        ]
        schema = ("run_id string, regra_id string, descricao string, dimensao string, alvo string, "
                  "escopo string, severidade string, criterio string, tratamento string, avaliados long, "
                  "falhas long, pct_falha double, valor_impactado_brl double, resultado string, detalhes string")
        return self.spark.createDataFrame(linhas, schema)

    def flush(self, schema_dq: str) -> None:
        """Grava log, quarentena e correções.

        - rule_results: APPEND (histórico de execuções -> tendência no monitoramento)
        - quarantine / corrections: substitui só os alvos desta execução (replaceWhere).
          Reexecutar uma etapa não duplica a quarentena dela, nem apaga a de outra etapa.
        """
        (self.resultados_df().withColumn("executado_em", F.current_timestamp())
             .write.format("delta").mode("append").option("mergeSchema", "true")
             .saveAsTable(f"{schema_dq}.rule_results"))

        alvos = sorted({r.alvo for r in self._resultados})
        filtro = "alvo IN ({})".format(", ".join(f"'{a}'" for a in alvos))
        for nome, frames in (("quarantine", self._quarentena), ("corrections", self._correcoes)):
            tabela = f"{schema_dq}.{nome}"
            if not frames:
                # nada a gravar nesta execução: limpa o que uma execução anterior deixou para estes alvos
                if self.spark.catalog.tableExists(tabela):
                    self.spark.sql(f"DELETE FROM {tabela} WHERE {filtro}")
                continue
            df = reduce(lambda a, b: a.unionByName(b), frames)
            writer = df.write.format("delta").option("mergeSchema", "true")
            if self.spark.catalog.tableExists(tabela):
                writer.mode("overwrite").option("replaceWhere", filtro).saveAsTable(tabela)
            else:
                writer.mode("overwrite").saveAsTable(tabela)