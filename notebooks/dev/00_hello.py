# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Hello serverless
# MAGIC Valida o ciclo: editar no VS Code → `bundle deploy` → rodar no Databricks.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
catalog = dbutils.widgets.get("catalog")
landing = f"/Volumes/{catalog}/ms_raw/landing"
print("Spark", spark.version, "| landing:", landing)

# COMMAND ----------

display(dbutils.fs.ls(landing))

# COMMAND ----------

df = spark.read.option("header", True).csv(f"{landing}/dim_calendario.csv")
print("linhas:", df.count())     # esperado: 91 (README do case)
print("colunas:", df.columns)    # OLHE COM ATENÇÃO o nome da 1ª coluna
display(df.limit(5))