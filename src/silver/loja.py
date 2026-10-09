"""Silver · dimensão de loja.

Sequência de regras:
  LOJ_001  store_id único                         -> quarentena
           referência cidade -> UF                -> DERIVADA dos dados (UF dominante)
  LOJ_002  UF coerente com a cidade               -> corrige pela referência
  LOJ_003  CNPJ sem máscara                       -> corrige (só formatação)
  LOJ_004  coordenada fora do Brasil (IBGE)       -> alerta, NÃO corrige
  LOJ_005  coordenada x cidade (dataset)          -> alerta: campo não confiável
  LOJ_006  território vazio                       -> alerta
  LOJ_007  vendedor vazio                         -> alerta
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.engine import DQEngine
from silver.produto import mapa

ALVO = "dim_loja"


def vazio(col: str):
    return F.col(col).isNull() | (F.trim(F.col(col)) == "")


class LojaBuilder:
    def __init__(self, spark: SparkSession, engine: DQEngine, referencias: dict):
        self.spark = spark
        self.engine = engine
        self.ref = referencias["loja"]
        self.ref_cidade_uf: DataFrame | None = None  # referência derivada, com evidência (vira tabela)

    # ------------------------------------------------------------------ referência cidade -> UF
    def derivar_uf(self, df: DataFrame) -> dict[str, str]:
        """UF dominante por cidade, contando só lojas com UF do domínio oficial."""
        contagem = (df.where(F.col("state").isin(self.ref["ufs_validas"]))
                      .groupBy("city", "state").agg(F.count("*").alias("lojas")))
        w = Window.partitionBy("city")
        self.ref_cidade_uf = (
            contagem
            .withColumn("total_cidade", F.sum("lojas").over(w))
            .withColumn("posicao", F.row_number().over(w.orderBy(F.desc("lojas"), "state")))
            .where("posicao = 1")
            .withColumn("pct_evidencia", F.round(F.col("lojas") / F.col("total_cidade"), 4))
            .withColumn("status", F.when(F.col("pct_evidencia") >= self.ref["uf_evidencia_minima"],
                                         "APROVADO").otherwise("REVISAR"))
            .select(F.col("city").alias("cidade"), F.col("state").alias("uf_oficial"),
                    F.col("lojas").alias("lojas_que_confirmam"), "total_cidade", "pct_evidencia", "status")
        )
        linhas = self.ref_cidade_uf.where("status = 'APROVADO'").collect()  # 1 linha por cidade
        return {r["cidade"]: r["uf_oficial"] for r in linhas}

    # ------------------------------------------------------------------ LOJ_005 (dataset)
    def _coordenada_vs_cidade(self, df: DataFrame, dentro) -> None:
        """Se a coordenada representasse a loja, o centro médio mudaria de cidade para cidade.

        Mede a variação dos centros entre cidades contra a dispersão dentro de cada cidade.
        """
        base = (df.where(dentro)
                  .select("city", F.col("latitude").cast("double").alias("lat"),
                          F.col("longitude").cast("double").alias("lon")))
        por_cidade = base.groupBy("city").agg(F.avg("lat").alias("lat_m"), F.avg("lon").alias("lon_m"),
                                              F.stddev("lat").alias("lat_dp"))
        r = por_cidade.agg((F.max("lat_m") - F.min("lat_m")).alias("amp_lat"),
                           (F.max("lon_m") - F.min("lon_m")).alias("amp_lon"),
                           F.avg("lat_dp").alias("dp_lat")).first()
        # correlação com try_divide: coordenada constante não pode derrubar o pipeline (ANSI)
        c = base.agg(F.try_divide(F.covar_pop("lat", "lon"),
                                  F.stddev_pop("lat") * F.stddev_pop("lon")).alias("c")).first()["c"]
        corr = c if c is not None else 0.0
        n = base.count()
        amp, dp = r["amp_lat"] or 0.0, r["dp_lat"] or 0.0
        # centros quase iguais (variação menor que a dispersão interna) => coordenada não acompanha a cidade
        nao_confiavel = dp > 0 and amp < dp
        self.engine.record(
            "LOJ_005", ALVO, avaliados=n, falhas=n if nao_confiavel else 0,
            detalhes=(f"centro médio por cidade varia {amp:.2f}° (lat) e {(r['amp_lon'] or 0.0):.2f}° (lon); "
                      f"dispersão interna {dp:.2f}°; correlação lat x lon {corr:.3f}. "
                      "Campo fora do Market Share; melhoria: endereço + CEP + geocodificação"),
        )

    # ------------------------------------------------------------------ pipeline
    def build(self, bronze: DataFrame) -> DataFrame:
        e = self.engine
        chave = ["store_id"]
        df = bronze.drop("_load_id")

        # LOJ_001 — store_id único
        df = e.quarantine(df, "LOJ_001", F.count("*").over(Window.partitionBy("store_id")) > 1,
                          alvo=ALVO, chave=chave, col_valor=None)

        # LOJ_002 — UF coerente com a cidade (referência derivada dos dados)
        uf_oficial = mapa(self.derivar_uf(df))[F.col("city")]
        incoerente = uf_oficial.isNotNull() & (F.col("state").isNull() | (F.col("state") != uf_oficial))
        df = e.correct(df, "LOJ_002", incoerente, {"state": uf_oficial}, alvo=ALVO, chave=chave, col_valor=None)

        # LOJ_003 — CNPJ: remove a máscara somente quando sobram exatamente 14 dígitos
        digitos = F.regexp_replace(F.col("cnpj"), r"\D", "")
        mascarado = ~F.col("cnpj").rlike(r"^\d{14}$") & (F.length(digitos) == 14)
        df = e.correct(df, "LOJ_003", mascarado, {"cnpj": digitos}, alvo=ALVO, chave=chave, col_valor=None)

        # LOJ_004 — coordenada fora do Brasil: alerta, sem alterar o dado
        lim = self.ref["limites_brasil"]
        lat, lon = F.col("latitude").cast("double"), F.col("longitude").cast("double")
        dentro = lat.between(lim["lat_sul"], lim["lat_norte"]) & lon.between(lim["lon_oeste"], lim["lon_leste"])
        invertida = lon.between(lim["lat_sul"], lim["lat_norte"]) & lat.between(lim["lon_oeste"], lim["lon_leste"])
        df = e.check(df, "LOJ_004", ~dentro, alvo=ALVO, col_valor=None,
                     detalhes="sem correção: indício de inversão, mas sem referência para confirmar a localização")
        df = df.withColumn("geo_status", F.when(~dentro & invertida, "REVALIDAR_INVERSAO")
                                          .when(~dentro, "REVALIDAR")
                                          .otherwise("NAO_VALIDADA"))  # nenhuma coordenada é validada hoje

        # LOJ_005 — coordenada x cidade (regra de dataset)
        self._coordenada_vs_cidade(df, dentro)

        # LOJ_006 / LOJ_007 — completude
        df = e.check(df, "LOJ_006", vazio("territory_id"), alvo=ALVO, col_valor=None)
        df = e.check(df, "LOJ_007", vazio("seller_id"), alvo=ALVO, col_valor=None)

        # Tipos de negócio (a Bronze é toda string)
        df = (df.withColumn("latitude", lat).withColumn("longitude", lon)
                .withColumn("active_flag", F.col("active_flag") == "Y"))
        return e.com_status(df)
