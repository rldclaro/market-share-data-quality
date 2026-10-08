# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver · Dimensões
# MAGIC
# MAGIC Trata as dimensões aplicando as regras de DQ do catálogo (`config/dq_rules.yml`).
# MAGIC Toda correção usa uma referência explícita (`config/referencias.yml`) e fica auditada.
# MAGIC
# MAGIC | Seção | Saída |
# MAGIC |---|---|
# MAGIC | Produto | `ms_silver.dim_produto` + `ms_silver.ref_hierarquia_produto` |

# COMMAND ----------

import os
import sys

import yaml

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("load_id", "manual")
dbutils.widgets.text("root", "")   # o job passa ${workspace.file_path}; no modo interativo, procura o databricks.yml



def raiz_do_projeto(inicio: str) -> str:
    """Sobe as pastas até achar o databricks.yml (funciona em qualquer nível de subpasta)."""
    p = inicio
    while p != "/" and not os.path.exists(os.path.join(p, "databricks.yml")):
        p = os.path.dirname(p)
    return p


catalog = dbutils.widgets.get("catalog")
load_id = dbutils.widgets.get("load_id")
root = dbutils.widgets.get("root") or raiz_do_projeto(os.getcwd())
sys.path.insert(0, f"{root}/src")

BRONZE, SILVER, DQ = f"{catalog}.ms_bronze", f"{catalog}.ms_silver", f"{catalog}.ms_dq"

# COMMAND ----------

from pyspark.sql import functions as F

from dq.engine import DQEngine
from dq.models import CatalogoRegras
from silver.produto import ProdutoBuilder

catalogo = CatalogoRegras.from_yaml(f"{root}/config/dq_rules.yml")
with open(f"{root}/config/referencias.yml", encoding="utf-8") as fh:
    referencias = yaml.safe_load(fh)

engine = DQEngine(spark, catalogo, run_id=load_id)
print(f"{len(catalogo)} regras | run_id={load_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Produto

# COMMAND ----------

produto = ProdutoBuilder(spark, engine, referencias)
dim_produto = produto.build(spark.table(f"{BRONZE}.dim_produto"))

(dim_produto.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{SILVER}.dim_produto"))
(produto.hierarquia.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
                   .saveAsTable(f"{SILVER}.ref_hierarquia_produto"))

display(spark.table(f"{SILVER}.ref_hierarquia_produto"))

# COMMAND ----------

# Garantias da dimensão: 1 linha por EAN (senão o join com a fato duplica venda)
dp = spark.table(f"{SILVER}.dim_produto")
linhas, eans = dp.count(), dp.select("ean").distinct().count()
print(f"dim_produto: {linhas} linhas | {eans} EANs distintos")
assert linhas == eans, "EAN duplicado na dimensão de produto"

display(dp.groupBy("dq_status").count().orderBy("dq_status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log das regras e gravação

# COMMAND ----------

display(engine.resultados_df())
engine.flush(DQ)