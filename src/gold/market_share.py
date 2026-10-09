"""Gold · Market Share multinível com portões de publicação.

Fórmula (por célula = semana x categoria x recorte):
    MS_valor(marca)  = Σ sales_value_brl(marca) / Σ sales_value_brl(todas as marcas da categoria)
    MS_volume(marca) = Σ sold_volume_kg(marca)  / Σ sold_volume_kg(todas as marcas da categoria)

Mesma base no numerador e no denominador: só entram registros APROVADO, CORRIGIDO_AUTOMATICAMENTE
e APROVADO_COM_ALERTA. Quarentena e rejeitados ficam fora dos dois lados.

Uma célula só é OFICIAL se: semana fechada, cobertura declarada >= 80% e quarentena <= 5%.
Caso contrário o share é calculado e publicado, mas `ms_value_official` fica NULO e `ms_status`
diz o motivo — o consumidor nunca recebe um número incompleto sem aviso.

Joins: as tabelas de qualidade por célula (cobertura e quarentena por semana x recorte) são
pequenas e vão em broadcast contra o agregado da fato.
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.engine import DQEngine
from dq.models import CONSUMIVEIS

COLUNAS_FATO = [
    "year_week", "week_start_date", "week_end_date", "year", "month", "provider", "store_id",
    "retailer_name", "channel", "state", "city", "territory_id", "territory_source", "seller_id",
    "product_id", "ean", "category", "subcategory", "brand", "manufacturer", "is_own_product",
    "units", "sales_value_brl", "sold_volume_kg", "unit_price", "anomaly_class", "series_ratio",
    "week_open_at_ingestion", "dq_status", "dq_flags", "source_file", "_row_hash",
]

CHAVE_QUARENTENA = "provider string, year_week string, store_id string, ean string, _row_hash string"


class MarketShareBuilder:
    def __init__(self, spark: SparkSession, engine: DQEngine, referencias: dict):
        self.spark = spark
        self.engine = engine
        self.ref = referencias["gold"]
        self.sem_territorio = referencias["fato"]["sem_territorio"]

    # ------------------------------------------------------------------ fato de consumo
    def fato_consumo(self, fato_silver: DataFrame) -> DataFrame:
        return fato_silver.where(F.col("dq_status").isin(*CONSUMIVEIS)).select(*COLUNAS_FATO)

    # ------------------------------------------------------------------ qualidade por célula
    def _qualidade(self, fato: DataFrame, quarentena: DataFrame, loja: DataFrame,
                   cobertura_rede: DataFrame, cols: list[str]) -> DataFrame:
        """Cobertura e % de quarentena por semana x recorte.

        Cobertura do recorte = média das redes ponderada pelo nº de lojas de cada rede no recorte.
        Quarentena: lojas inexistentes (FCT_004) não têm recorte, então só contam no NACIONAL.
        """
        dims = loja.select("store_id", "retailer_name", "channel", "state",
                           F.coalesce("territory_id", F.lit(self.sem_territorio)).alias("territory_id"))
        pesos = dims.groupBy("retailer_name", *cols).agg(F.count("*").alias("_lojas"))
        cob = (cobertura_rede.join(F.broadcast(pesos), "retailer_name")
               .groupBy("year_week", *cols)
               .agg(F.round(F.sum(F.col("coverage_ratio") * F.col("_lojas")) / F.sum("_lojas"), 4).alias("coverage_ratio")))

        q = (quarentena.where((F.col("alvo") == "fato_vendas") & (F.col("classificacao") == "QUARENTENA"))
                       .withColumn("_k", F.from_json("chave", CHAVE_QUARENTENA))
                       .select(F.col("_k.year_week").alias("year_week"), F.col("_k.store_id").alias("store_id")))
        if cols:
            q = q.join(F.broadcast(dims), "store_id")
        q = q.groupBy("year_week", *cols).agg(F.count("*").alias("quarantined_rows"))
        ok = fato.groupBy("year_week", *cols).agg(F.count("*").alias("consumable_rows"))
        qual = (ok.join(q, ["year_week", *cols], "full")
                  .fillna({"quarantined_rows": 0, "consumable_rows": 0})
                  .withColumn("quarantine_row_pct", F.round(
                      F.col("quarantined_rows") / (F.col("quarantined_rows") + F.col("consumable_rows")), 4)))
        return qual.join(cob, ["year_week", *cols], "left")

    # ------------------------------------------------------------------ market share
    def market_share(self, fato: DataFrame, quarentena: DataFrame, loja: DataFrame,
                     cobertura_rede: DataFrame) -> DataFrame:
        semanas_abertas = [r[0] for r in fato.where("week_open_at_ingestion")
                                             .select("year_week").distinct().collect()]  # poucas semanas
        frames = []
        for nivel, cols in self.ref["niveis"].items():
            g = (fato.groupBy("year_week", "category", "brand", "manufacturer", "is_own_product", *cols)
                     .agg(F.sum("sales_value_brl").alias("brand_value_brl"),
                          F.sum("sold_volume_kg").alias("brand_volume_kg"),
                          F.sum("units").alias("brand_units"),
                          F.coalesce(F.sum(F.when(F.col("dq_status") == "APROVADO_COM_ALERTA", F.col("sales_value_brl"))),
                                     F.lit(0).cast("decimal(18,2)")).alias("brand_value_alert_brl"),
                          F.countDistinct("store_id").alias("brand_stores")))
            w = Window.partitionBy("year_week", "category", *cols)
            g = (g.withColumn("category_value_brl", F.sum("brand_value_brl").over(w))
                  .withColumn("category_volume_kg", F.sum("brand_volume_kg").over(w))
                  .withColumn("category_value_alert_brl", F.sum("brand_value_alert_brl").over(w))
                  # razão de DECIMAIS (exata) arredondada a 8 casas -> determinística entre execuções
                  .withColumn("ms_value", F.when(F.col("category_value_brl") > 0, F.round(
                      F.col("brand_value_brl") / F.col("category_value_brl"), 8).cast("double")))
                  .withColumn("ms_volume", F.when(F.col("category_volume_kg") > 0, F.round(
                      F.col("brand_volume_kg") / F.col("category_volume_kg"), 8).cast("double")))
                  .withColumn("ms_rank", F.dense_rank().over(w.orderBy(F.col("brand_value_brl").desc()))))
            qual = self._qualidade(fato, quarentena, loja, cobertura_rede, cols)
            g = g.join(F.broadcast(qual), ["year_week", *cols], "left")
            valor_nivel = F.concat_ws("|", *[F.col(c) for c in cols]) if cols else F.lit("BRASIL")
            frames.append(g.withColumn("level", F.lit(nivel)).withColumn("level_value", valor_nivel).drop(*cols))

        ms = reduce(lambda a, b: a.unionByName(b), frames)
        r = self.ref
        aberta = F.col("year_week").isin(*semanas_abertas) if semanas_abertas else F.lit(False)
        status = (F.when(aberta, "NAO_OFICIAL_SEMANA_ABERTA")
                   .when(F.coalesce(F.col("coverage_ratio"), F.lit(0.0)) < r["cobertura_minima"], "NAO_OFICIAL_COBERTURA")
                   .when(F.col("quarantine_row_pct") > r["quarentena_maxima_pct"], "NAO_OFICIAL_QUARENTENA")
                   .when(F.col("ms_value").isNull(), "SEM_VENDA_NA_CATEGORIA")
                   .otherwise("OFICIAL"))
        ms = (ms.withColumn("ms_status", status)
                .withColumn("ms_value_official", F.when(F.col("ms_status") == "OFICIAL", F.col("ms_value")))
                .withColumn("value_alert_pct", F.when(F.col("category_value_brl") > 0, F.round(
                    F.col("category_value_alert_brl") / F.col("category_value_brl"), 4).cast("double"))))
        return ms.select("level", "level_value", "year_week", "category", "brand", "manufacturer", "is_own_product",
                         "brand_value_brl", "category_value_brl", "ms_value", "ms_value_official",
                         "brand_volume_kg", "category_volume_kg", "ms_volume", "brand_units", "ms_rank",
                         "brand_stores", "coverage_ratio", "quarantine_row_pct", "value_alert_pct", "ms_status")

    # ------------------------------------------------------------------ portões de publicação
    def portoes(self, linhas_bronze: int, fato_silver: DataFrame, fato_gold: DataFrame,
                ms: DataFrame, quarentena: DataFrame) -> list[str]:
        """Registra GLD_001..005 e devolve a lista de portões CRÍTICOS que falharam."""
        e = self.engine
        retiradas = quarentena.where(F.col("alvo") == "fato_vendas").count()
        silver = fato_silver.count()
        r1 = e.record("GLD_001", "gold", avaliados=linhas_bronze,
                      falhas=abs(linhas_bronze - (silver + retiradas)),
                      detalhes=f"bronze={linhas_bronze} silver={silver} quarentena+rejeitados={retiradas}")

        sv = fato_silver.where(F.col("dq_status").isin(*CONSUMIVEIS)).agg(F.sum("sales_value_brl"), F.count("*")).first()
        gv = fato_gold.agg(F.sum("sales_value_brl"), F.count("*")).first()
        nac = ms.where(F.col("level") == "NACIONAL").agg(F.sum("brand_value_brl")).first()[0]
        dif = abs(float(sv[0] or 0) - float(gv[0] or 0)) + abs(float(gv[0] or 0) - float(nac or 0))
        r2 = e.record("GLD_002", "gold", avaliados=gv[1], falhas=int(dif > 0.01) + abs(sv[1] - gv[1]),
                      detalhes=f"Σsilver={float(sv[0]):.2f} Σgold={float(gv[0]):.2f} Σms_nacional={float(nac):.2f}")

        celulas = (ms.where(F.col("ms_value").isNotNull())
                     .groupBy("level", "level_value", "year_week", "category").agg(F.sum("ms_value").alias("s")))
        st = celulas.agg(F.count("*").alias("n"),
                         F.sum((F.abs(F.col("s") - 1) > self.ref["tolerancia_soma_ms"]).cast("int")).alias("ruins")).first()
        r3 = e.record("GLD_003", "market_share", avaliados=st["n"], falhas=st["ruins"] or 0)

        dist = {x["ms_status"]: x["count"] for x in ms.groupBy("ms_status").count().collect()}
        n = sum(dist.values())
        e.record("GLD_004", "market_share", avaliados=n,
                 falhas=n - dist.get("OFICIAL", 0) - dist.get("SEM_VENDA_NA_CATEGORIA", 0),
                 detalhes=", ".join(f"{k}={v}" for k, v in sorted(dist.items())))

        colunas = set(fato_gold.columns) | set(ms.columns)
        vazadas = sorted(colunas & set(self.ref["colunas_proibidas"]))
        r5 = e.record("GLD_005", "gold", avaliados=len(colunas), falhas=len(vazadas), detalhes=f"vazadas={vazadas}")

        return [r.regra.id for r in (r1, r2, r3, r5) if r.falhas > 0]