# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold · Cobertura e Market Share
# MAGIC
# MAGIC 1. **Silver · cobertura** dos fornecedores (`ms_silver.cobertura_fornecedor`, regras COV_*)
# MAGIC 2. **Gold** materializada em Delta: `fato_vendas`, `cobertura_semana_rede`, `market_share`
# MAGIC 3. **Portões de publicação** (GLD_*): o Market Share é calculado numa tabela de staging, validado e só
# MAGIC    então publicado. Se um portão crítico falha, a versão anterior da Gold continua no ar.
# MAGIC 4. **Views de consumo**: `vw_market_share_oficial`, `vw_share_nestle`, `vw_cobertura_fornecedor`

# COMMAND ----------

import os
import sys

import yaml

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("load_id", "manual")
dbutils.widgets.text("root", "")


def raiz_do_projeto(inicio: str) -> str:
    """Sobe as pastas até achar o databricks.yml."""
    p = inicio
    while p != "/" and not os.path.exists(os.path.join(p, "databricks.yml")):
        p = os.path.dirname(p)
    return p


catalog = dbutils.widgets.get("catalog")
load_id = dbutils.widgets.get("load_id")
root = dbutils.widgets.get("root") or raiz_do_projeto(os.getcwd())
sys.path.insert(0, f"{root}/src")

BRONZE, SILVER, GOLD, DQ = (f"{catalog}.ms_bronze", f"{catalog}.ms_silver",
                            f"{catalog}.ms_gold", f"{catalog}.ms_dq")

# COMMAND ----------

from pyspark.sql import functions as F

from dq.engine import DQEngine
from dq.models import CatalogoRegras
from gold.market_share import MarketShareBuilder
from silver.cobertura import CoberturaBuilder

catalogo = CatalogoRegras.from_yaml(f"{root}/config/dq_rules.yml")
with open(f"{root}/config/referencias.yml", encoding="utf-8") as fh:
    referencias = yaml.safe_load(fh)

engine = DQEngine(spark, catalogo, run_id=load_id)


def gravar(df, tabela):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela)
    return spark.table(tabela)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver · cobertura dos fornecedores

# COMMAND ----------

fornecedores = referencias["fato"]["fornecedores"]
bronze_fato = {s["tabela"]: spark.table(f"{BRONZE}.{s['tabela']}") for s in fornecedores.values()}
loja = spark.table(f"{SILVER}.dim_loja")

cobertura = gravar(
    CoberturaBuilder(spark, engine).build(
        spark.table(f"{BRONZE}.coverage_provider"),
        calendario=spark.table(f"{SILVER}.dim_calendario"),
        loja=loja,
        fato_bronze=CoberturaBuilder.fato_observada(bronze_fato, fornecedores),
    ),
    f"{SILVER}.cobertura_fornecedor",
)
display(cobertura.groupBy("provider", "dq_status").count().orderBy("provider", "dq_status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold · fato de consumo e cobertura por semana x rede

# COMMAND ----------

ms_builder = MarketShareBuilder(spark, engine, referencias)
fato_silver = spark.table(f"{SILVER}.fato_vendas")
fato_gold = gravar(ms_builder.fato_consumo(fato_silver), f"{GOLD}.fato_vendas")
cobertura_rede = gravar(CoberturaBuilder.por_semana_rede(cobertura), f"{GOLD}.cobertura_semana_rede")

print(f"Gold · fato_vendas: {fato_gold.count()} linhas consumíveis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Market Share (staging -> portões -> publicação)

# COMMAND ----------

quarentena = spark.table(f"{DQ}.quarantine")
ms_stg = gravar(ms_builder.market_share(fato_gold, quarentena, loja, cobertura_rede), f"{GOLD}.stg_market_share")

linhas_bronze = sum(df.count() for df in bronze_fato.values())
falhas_criticas = ms_builder.portoes(linhas_bronze, fato_silver, fato_gold, ms_stg, quarentena)

display(engine.resultados_df())
engine.flush(DQ)   # o log é gravado mesmo se a publicação for bloqueada

assert not falhas_criticas, f"Publicação bloqueada pelos portões: {falhas_criticas}"

spark.sql(f"CREATE OR REPLACE TABLE {GOLD}.market_share AS SELECT * FROM {GOLD}.stg_market_share")
spark.sql(f"DROP TABLE IF EXISTS {GOLD}.stg_market_share")

display(spark.table(f"{GOLD}.market_share").groupBy("level", "ms_status").count().orderBy("level", "ms_status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views de consumo
# MAGIC Contrato estável para o BI: só o que é oficial, colunas com nome de negócio e acesso mínimo
# MAGIC (o consumidor recebe permissão na view, não na tabela).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.vw_market_share_oficial
COMMENT 'Market Share oficial: semana fechada, cobertura >= 80% e quarentena <= 5%'
AS SELECT level AS nivel, level_value AS recorte, year_week AS semana, category AS categoria,
          brand AS marca, manufacturer AS fabricante, is_own_product AS produto_proprio,
          ms_value_official AS market_share_valor, ms_volume AS market_share_volume,
          brand_value_brl AS venda_marca_brl, category_value_brl AS venda_categoria_brl,
          ms_rank AS ranking, coverage_ratio AS cobertura, value_alert_pct AS pct_valor_em_alerta
   FROM {GOLD}.market_share
   WHERE ms_status = 'OFICIAL'
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.vw_share_nestle
COMMENT 'Share dos produtos próprios (Nestlé) por semana e categoria, com o status de publicação'
AS SELECT level AS nivel, level_value AS recorte, year_week AS semana, category AS categoria,
          SUM(brand_value_brl)                          AS venda_nestle_brl,
          MAX(category_value_brl)                       AS venda_categoria_brl,
          ROUND(SUM(brand_value_brl) / MAX(category_value_brl), 6)   AS share_nestle,
          ROUND(SUM(ms_value_official), 6)              AS share_nestle_oficial,
          MAX(ms_status)                                AS status
   FROM {GOLD}.market_share
   WHERE is_own_product = 'Y'
   GROUP BY level, level_value, year_week, category
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.vw_cobertura_fornecedor
COMMENT 'Cobertura declarada por fornecedor x semana x rede, com alertas do metadado'
AS SELECT provider AS fornecedor, year_week AS semana, retailer_name AS rede,
          expected_stores AS lojas_esperadas, received_stores AS lojas_declaradas,
          effective_received_stores AS lojas_consideradas, observed_stores AS lojas_na_fato,
          coverage_ratio AS cobertura, file_received AS arquivo_recebido,
          delivery_delay_days AS atraso_entrega_dias, dq_flags AS alertas, dq_status AS status
   FROM {SILVER}.cobertura_fornecedor
""")

display(spark.sql(f"SELECT * FROM {GOLD}.vw_share_nestle WHERE nivel = 'NACIONAL' ORDER BY semana DESC, categoria LIMIT 20"))