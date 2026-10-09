"""Silver · cobertura declarada pelos fornecedores (fornecedor x semana x rede).

É a base para separar AUSÊNCIA DE VENDA de AUSÊNCIA DE COBERTURA:
  * loja coberta e sem linha do produto      -> venda zero (real)
  * rede/semana sem cobertura suficiente     -> dado faltante (não é zero!) -> Market Share não oficial

Cobertura efetiva (conservadora):
  arquivo não recebido -> 0 | recebidas limitadas ao esperado (nunca > 100%)
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.engine import DQEngine

ALVO = "cobertura"
CHAVE = ["provider", "year_week", "retailer_name"]


class CoberturaBuilder:
    def __init__(self, spark: SparkSession, engine: DQEngine):
        self.spark = spark
        self.engine = engine

    def build(self, bronze: DataFrame, calendario: DataFrame, loja: DataFrame, fato_bronze: DataFrame) -> DataFrame:
        """`fato_bronze` = fato harmonizada ANTES das regras: o metadado é confrontado com tudo o que chegou."""
        e = self.engine
        df = (bronze.drop("_load_id")
                    .withColumn("expected_stores", F.col("expected_stores").cast("int"))
                    .withColumn("received_stores", F.col("received_stores").cast("int"))
                    .withColumn("expected_delivery_date", F.to_date("expected_delivery_date"))
                    .withColumn("actual_delivery_date", F.to_date("actual_delivery_date")))

        # COV_001 — chave única
        df = e.quarantine(df, "COV_001", F.count("*").over(Window.partitionBy(*CHAVE)) > 1,
                          alvo=ALVO, chave=CHAVE, col_valor=None)

        # COV_007 — rede e semana existem nas dimensões (broadcast: dimensões pequenas)
        redes = loja.select("retailer_name").distinct().withColumn("_rede_ok", F.lit(True))
        cal = calendario.select("year_week", "week_end_date")
        df = df.join(F.broadcast(redes), "retailer_name", "left").join(F.broadcast(cal), "year_week", "left")
        df = e.quarantine(df, "COV_007", F.col("_rede_ok").isNull() | F.col("week_end_date").isNull(),
                          alvo=ALVO, chave=CHAVE, col_valor=None).drop("_rede_ok")

        # COV_002 / 003 / 005 / 006 — consistência e prazo
        df = e.check(df, "COV_002", F.col("received_stores") > F.col("expected_stores"), alvo=ALVO, col_valor=None)
        df = e.check(df, "COV_003", (F.col("file_received") == "Y") & (F.col("received_stores") == 0),
                     alvo=ALVO, col_valor=None)
        df = e.check(df, "COV_005", F.col("actual_delivery_date") > F.col("expected_delivery_date"),
                     alvo=ALVO, col_valor=None)
        df = e.check(df, "COV_006", F.col("expected_delivery_date") <= F.col("week_end_date"), alvo=ALVO,
                     col_valor=None, detalhes="entrega esperada = fim da semana - 3 dias; semântica a validar")

        # COV_004 / COV_008 — metadado x o que de fato chegou (broadcast: ~6 mil células)
        observado = (fato_bronze.join(F.broadcast(loja.select("store_id", "retailer_name")), "store_id")
                                .groupBy("provider", "year_week", "retailer_name")
                                .agg(F.countDistinct("store_id").alias("observed_stores"),
                                     F.count("*").alias("observed_rows")))
        df = (df.join(F.broadcast(observado), CHAVE, "left")
                .fillna({"observed_stores": 0, "observed_rows": 0}))
        df = e.check(df, "COV_004", (F.col("file_received") == "N") & (F.col("observed_rows") > 0),
                     alvo=ALVO, col_valor=None, detalhes="cobertura efetiva mantida em 0 (conservador)")
        df = e.check(df, "COV_008", F.col("observed_stores") > F.col("received_stores"),
                     alvo=ALVO, col_valor=None, detalhes="a fato traz só linhas com movimento")

        # Cobertura efetiva
        df = (df.withColumn("delivery_delay_days", F.datediff("actual_delivery_date", "expected_delivery_date"))
                .withColumn("effective_received_stores",
                            F.when(F.col("file_received") != "Y", F.lit(0))
                             .otherwise(F.least(F.col("received_stores"), F.col("expected_stores"))))
                .withColumn("coverage_ratio",
                            F.round(F.try_divide(F.col("effective_received_stores"), F.col("expected_stores")), 4)))
        return e.com_status(df)

    @staticmethod
    def por_semana_rede(cobertura: DataFrame) -> DataFrame:
        """Cobertura por semana x rede somando os dois fornecedores (fontes complementares)."""
        return (cobertura.groupBy("year_week", "retailer_name")
                .agg(F.sum("effective_received_stores").alias("effective_received_stores"),
                     F.sum("expected_stores").alias("expected_stores"),
                     F.collect_set(F.when(F.col("file_received") != "Y", F.col("provider"))).alias("providers_missing"))
                .withColumn("coverage_ratio",
                            F.round(F.try_divide(F.col("effective_received_stores"), F.col("expected_stores")), 4)))

    @staticmethod
    def fato_observada(bronze: dict[str, DataFrame], fornecedores: dict) -> DataFrame:
        """Fato da Bronze com só fornecedor, semana e loja (mapeamento de config/referencias.yml).

        É o que "chegou" de verdade, antes de qualquer regra: base da COV_004 / COV_008.
        """
        frames = [bronze[s["tabela"]].select(F.lit(p).alias("provider"),
                                             F.col(s["obrigatorias"]["year_week"]).alias("year_week"),
                                             F.col(s["obrigatorias"]["store_id"]).alias("store_id"))
                  for p, s in fornecedores.items()]
        out = frames[0]
        for f in frames[1:]:
            out = out.unionByName(f)
        return out