# Databricks notebook source
# MAGIC %md
# MAGIC # Demo · motor de Data Quality
# MAGIC
# MAGIC Valida o framework usando os dois problemas encontrados no profiling da Bronze:
# MAGIC - **FCT_001**: duplicatas exatas (esperado: 250 cópias em A)
# MAGIC - **FCT_002**: mesma chave com versões conflitantes (esperado: 22 linhas em A)
# MAGIC
# MAGIC Notebook de validação: não faz parte do pipeline. Grava com alvo `demo_fact_a` / run_id `demo`
# MAGIC e apaga tudo no final.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("root", "")   # raiz do projeto no workspace (o job passa ${workspace.file_path})


def raiz_do_projeto(inicio: str) -> str:
    """Sobe as pastas até achar o databricks.yml (funciona em qualquer nível de subpasta)."""
    p = inicio
    while p != "/" and not os.path.exists(os.path.join(p, "databricks.yml")):
        p = os.path.dirname(p)
    return p


catalog = dbutils.widgets.get("catalog")
root = dbutils.widgets.get("root") or raiz_do_projeto(os.getcwd())
sys.path.insert(0, f"{root}/src")
print("root:", root)

# COMMAND ----------

from pyspark.sql import Window, functions as F

from dq.engine import DQEngine
from dq.models import CatalogoRegras

catalogo = CatalogoRegras.from_yaml(f"{root}/config/dq_rules.yml")
engine = DQEngine(spark, catalogo, run_id="demo")
print(f"{len(catalogo)} regras carregadas:", [r.id for r in catalogo])

# COMMAND ----------

ALVO = "demo_fact_a"
CHAVE = ["week", "store_id", "ean", "_row_hash"]

df = spark.table(f"{catalog}.ms_bronze.fact_provider_a")

# FCT_001 — duplicata exata: numera as ocorrências de cada hash e mantém só a 1ª.
# Como as cópias são idênticas, qualquer uma pode ser a "1ª": a ordem não muda o resultado.
w_hash = Window.partitionBy("_row_hash").orderBy("_source_file")
df = df.withColumn("_ocorrencia", F.row_number().over(w_hash))
df = engine.quarantine(df, "FCT_001", F.col("_ocorrencia") > 1, alvo=ALVO, chave=CHAVE,
                       motivo="cópia de duplicata exata (mantida a 1ª ocorrência)")

# FCT_002 — depois de remover as cópias, a chave de negócio ainda aparece mais de 1 vez?
w_chave = Window.partitionBy("week", "store_id", "ean")
df = engine.quarantine(df, "FCT_002", F.count("*").over(w_chave) > 1, alvo=ALVO, chave=CHAVE,
                       motivo=F.concat(F.lit("versões conflitantes; arquivo="), F.col("_source_file")))

df = engine.com_status(df.drop("_ocorrencia"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log das regras (o que vai para `ms_dq.rule_results`)

# COMMAND ----------

display(engine.resultados_df())

# COMMAND ----------

engine.flush(f"{catalog}.ms_dq")
display(spark.sql(f"""
    SELECT regra_id, classificacao, severidade, COUNT(*) AS linhas, ROUND(SUM(valor_brl), 2) AS valor_brl
    FROM {catalog}.ms_dq.quarantine
    WHERE alvo = '{ALVO}'
    GROUP BY ALL ORDER BY regra_id
"""))

# COMMAND ----------

print("linhas que seguem no fluxo:", df.count())   # esperado: 68.526 - 250 - 22 = 68.254

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza
# MAGIC Remove o que a demo gravou, para não misturar com o pipeline.

# COMMAND ----------

spark.sql(f"DELETE FROM {catalog}.ms_dq.quarantine WHERE alvo = '{ALVO}'")
spark.sql(f"DELETE FROM {catalog}.ms_dq.rule_results WHERE run_id = 'demo'")
print("Dados da demo removidos.")