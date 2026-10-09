"""Silver · dimensão de calendário (grão semanal ISO 8601).

Sequência de regras:
  CAL_003  year_week único e coerente com o início   -> rejeita (bloqueia o pipeline)
  CAL_002  semana completa e contínua                -> rejeita (bloqueia o pipeline)
  CAL_001  ano no padrão ISO (quinta-feira)         -> corrige; original em year_raw
  CAL_004  mês no padrão ISO (quinta-feira)         -> corrige; original em month_raw

Por que a quinta-feira: é o dia do meio da semana (seg..dom). O ano e o mês dela são os que
contêm a maioria dos dias (4 de 7) — é a regra do ISO 8601, a mesma que gera o year_week.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.engine import DQEngine

ALVO = "dim_calendario"


class CalendarioBuilder:
    def __init__(self, spark: SparkSession, engine: DQEngine):
        self.spark = spark
        self.engine = engine

    def build(self, bronze: DataFrame) -> DataFrame:
        e = self.engine
        chave = ["year_week"]

        # Tipos de negócio primeiro (a Bronze é toda string): as regras comparam datas e números
        df = (bronze.drop("_load_id")
                    .withColumn("date", F.to_date("date"))
                    .withColumn("week_start_date", F.to_date("week_start_date"))
                    .withColumn("week_end_date", F.to_date("week_end_date"))
                    .withColumn("year", F.col("year").cast("int"))
                    .withColumn("month", F.col("month").cast("int"))
                    .withColumn("week", F.col("week").cast("int")))

        inicio = F.col("week_start_date")
        quinta = F.date_add(inicio, 3)

        # CAL_003 — year_week único e igual à semana ISO do início
        semana_iso = F.concat(F.year(quinta).cast("string"), F.lit("-"), F.lpad(F.weekofyear(inicio).cast("string"), 2, "0"))
        duplicada = F.count("*").over(Window.partitionBy("year_week")) > 1
        df = e.check(df, "CAL_003", duplicada | (F.col("year_week") != semana_iso), alvo=ALVO, col_valor=None)

        # CAL_002 — segunda a domingo, sem lacunas (o calendário inteiro cabe em uma partição: 91 linhas)
        anterior = F.lag(inicio).over(Window.orderBy(inicio))
        quebrada = ((F.dayofweek(inicio) != 2)                                   # 2 = segunda-feira
                    | (F.datediff(F.col("week_end_date"), inicio) != 6)
                    | (anterior.isNotNull() & (F.datediff(inicio, anterior) != 7)))
        df = e.check(df, "CAL_002", quebrada, alvo=ALVO, col_valor=None)

        # CAL_001 / CAL_004 — ano e mês ISO (da quinta-feira). Regras separadas: cada uma só
        # registra na trilha a coluna que de fato mudou
        df = e.correct(df, "CAL_001", F.col("year") != F.year(quinta), {"year": F.year(quinta)},
                       alvo=ALVO, chave=chave, col_valor=None,
                       detalhes="ano civil da segunda-feira -> ano ISO da quinta-feira")
        df = e.correct(df, "CAL_004", F.col("month") != F.month(quinta), {"month": F.month(quinta)},
                       alvo=ALVO, chave=chave, col_valor=None,
                       detalhes="mês civil da segunda-feira -> mês ISO da quinta-feira (4 dos 7 dias)")

        return e.com_status(df)
