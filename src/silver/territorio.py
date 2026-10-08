"""Silver · histórico de território (SCD2 por loja x categoria).

Sequência de regras:
  TER_004  loja existe na dim_loja                 -> quarentena
  TER_006  vigência única (loja x categoria x início) -> quarentena
  TER_001  vigências sobrepostas                   -> corrige: fecha em (próximo início - 1 dia)
  TER_002  território vazio                        -> alerta (fallback na fato)
  TER_005  vendedor vazio                          -> alerta
  TER_003  loja sem histórico (dataset)            -> alerta (fallback na fato)

O território usado na fato é resolvido "as-of" na Etapa 6:
  histórico vigente na semana (loja x categoria) -> dim_loja.territory_id -> SEM_TERRITORIO
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.engine import DQEngine
from silver.loja import vazio

ALVO = "territorio_historico"


class TerritorioBuilder:
    def __init__(self, spark: SparkSession, engine: DQEngine):
        self.spark = spark
        self.engine = engine

    def build(self, bronze: DataFrame, dim_loja: DataFrame) -> DataFrame:
        e = self.engine
        chave = ["store_id", "category", "valid_from"]
        df = (bronze.drop("_load_id")
                    .withColumn("valid_from", F.to_date("valid_from"))
                    .withColumn("valid_to", F.to_date("valid_to")))

        # TER_004 — loja do histórico precisa existir na dimensão
        lojas = dim_loja.select("store_id", F.lit(True).alias("_loja_existe"))
        df = df.join(lojas, "store_id", "left")
        df = e.quarantine(df, "TER_004", F.col("_loja_existe").isNull(), alvo=ALVO, chave=chave, col_valor=None)
        df = df.drop("_loja_existe")

        # TER_006 — duas vigências começando no mesmo dia não têm vencedora segura
        df = e.quarantine(df, "TER_006",
                          F.count("*").over(Window.partitionBy("store_id", "category", "valid_from")) > 1,
                          alvo=ALVO, chave=chave, col_valor=None)

        # TER_001 — sobreposição: a vigência que começa depois vence; a anterior é fechada na véspera
        w = Window.partitionBy("store_id", "category").orderBy("valid_from")
        proximo_inicio = F.lead("valid_from").over(w)
        df = df.withColumn("_proximo_inicio", proximo_inicio)
        sobreposta = F.col("_proximo_inicio").isNotNull() & (F.col("valid_to") >= F.col("_proximo_inicio"))
        df = e.correct(df, "TER_001", sobreposta, {"valid_to": F.date_sub(F.col("_proximo_inicio"), 1)},
                       alvo=ALVO, chave=chave, col_valor=None,
                       detalhes="vigência anterior fechada em (próximo início - 1 dia)")
        df = df.drop("_proximo_inicio")

        # TER_002 / TER_005 — completude
        df = e.check(df, "TER_002", vazio("territory_id"), alvo=ALVO, col_valor=None)
        df = e.check(df, "TER_005", vazio("seller_id"), alvo=ALVO, col_valor=None)

        # TER_003 — lojas da dimensão sem nenhuma vigência (regra de dataset)
        total = dim_loja.select("store_id").distinct().count()
        sem_hist = dim_loja.select("store_id").distinct().join(df.select("store_id").distinct(),
                                                               "store_id", "left_anti").count()
        e.record("TER_003", ALVO, avaliados=total, falhas=sem_hist,
                 detalhes="na fato o território vem de dim_loja.territory_id (fallback)")

        return e.com_status(df)