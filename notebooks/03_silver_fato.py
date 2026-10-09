# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver · Fato de vendas (A + B)
# MAGIC
# MAGIC Harmoniza os dois fornecedores num schema único, aplica as regras de DQ da fato
# MAGIC (`config/dq_rules.yml`, ING_* e FCT_*) e grava `ms_silver.fato_vendas`.
# MAGIC
# MAGIC Garantia principal: **conservação de registros** — toda linha da Bronze termina na Silver,
# MAGIC na quarentena ou rejeitada com justificativa. Nada some em silêncio.

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

BRONZE, SILVER, DQ = f"{catalog}.ms_bronze", f"{catalog}.ms_silver", f"{catalog}.ms_dq"

# COMMAND ----------

from pyspark.sql import functions as F

from dq.engine import DQEngine
from dq.models import CatalogoRegras
from silver.fato import ALVO, FatoBuilder

catalogo = CatalogoRegras.from_yaml(f"{root}/config/dq_rules.yml")
with open(f"{root}/config/referencias.yml", encoding="utf-8") as fh:
    referencias = yaml.safe_load(fh)

engine = DQEngine(spark, catalogo, run_id=load_id)


def checkpoint(df, nome):
    """Serverless não tem cache(): grava o intermediário em Delta e relê (corta a linhagem)."""
    tabela = f"{SILVER}.stg_{nome}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela)
    return spark.table(tabela)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Harmonização (A + B -> schema canônico)

# COMMAND ----------

fato = FatoBuilder(spark, engine, referencias, checkpoint)
fontes = {spec["tabela"]: spark.table(f"{BRONZE}.{spec['tabela']}")
          for spec in referencias["fato"]["fornecedores"].values()}
canonica = fato.harmonizar(fontes)
linhas_bronze = canonica.count()
print(f"Bronze (A + B): {linhas_bronze} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras de DQ e gravação da Silver

# COMMAND ----------

fato_vendas = fato.build(
    canonica,
    produto=spark.table(f"{SILVER}.dim_produto"),
    loja=spark.table(f"{SILVER}.dim_loja"),
    territorio=spark.table(f"{SILVER}.territorio_historico"),
    calendario=spark.table(f"{SILVER}.dim_calendario"),
)

(fato_vendas.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{SILVER}.fato_vendas"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log das regras e gravação

# COMMAND ----------

display(engine.resultados_df())
engine.flush(DQ)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Garantias

# COMMAND ----------

fv = spark.table(f"{SILVER}.fato_vendas")
linhas_silver = fv.count()
linhas_retiradas = spark.table(f"{DQ}.quarantine").where(F.col("alvo") == ALVO).count()
print(f"Bronze {linhas_bronze} = Silver {linhas_silver} + quarentena/rejeitados {linhas_retiradas}")
assert linhas_bronze == linhas_silver + linhas_retiradas, "conservação de registros violada"

chaves = fv.select("year_week", "store_id", "ean").distinct().count()
print(f"Chaves semana x loja x EAN: {chaves} | linhas: {linhas_silver}")
assert chaves == linhas_silver, "chave de negócio duplicada na Silver"

negativos = fv.where("sales_value_brl < 0").count()
assert negativos == 0, "valor negativo na Silver"

display(fv.groupBy("provider", "dq_status").count().orderBy("provider", "dq_status"))
display(fv.groupBy("territory_source").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fila de tratativa da quarentena
# MAGIC Uma linha por regra x fornecedor x semana: quanto está retido, exemplo de motivo, o que fazer e
# MAGIC quem resolve (`config/referencias.yml -> fato.tratativa_quarentena`). Quando o fornecedor reenvia
# MAGIC o arquivo corrigido, a reexecução do job tira o registro da quarentena sozinha (replaceWhere por alvo).

# COMMAND ----------

def sql_texto(v: str) -> str:
    return "'" + str(v).replace("'", "''") + "'"

tratativa = ",\n    ".join(
    f"({sql_texto(r)}, {sql_texto(t['acao'])}, {sql_texto(t['responsavel'])})"
    for r, t in referencias["fato"]["tratativa_quarentena"].items()
)

spark.sql(f"""
CREATE OR REPLACE VIEW {DQ}.vw_quarentena_fato AS
WITH tratativa AS (
  SELECT * FROM VALUES
    {tratativa}
  AS t(regra_id, acao_sugerida, responsavel)
)
SELECT q.regra_id, q.classificacao, q.severidade,
       get_json_object(q.chave, '$.provider')  AS provider,
       get_json_object(q.chave, '$.year_week') AS year_week,
       COUNT(*)                               AS linhas,
       ROUND(SUM(abs(q.valor_brl)), 2)        AS valor_retido_brl,   -- abs: mesmo critério do valor_impactado_brl do log
       MIN(q.motivo)                          AS exemplo_motivo,
       COLLECT_SET(q.arquivo_origem)          AS arquivos,
       coalesce(t.acao_sugerida, 'Analisar')  AS acao_sugerida,
       coalesce(t.responsavel, 'DADOS')       AS responsavel,
       MAX(q.run_id)                          AS run_id
FROM {DQ}.quarantine q
LEFT JOIN tratativa t ON t.regra_id = q.regra_id
WHERE q.alvo = '{ALVO}'
GROUP BY q.regra_id, q.classificacao, q.severidade, provider, year_week, t.acao_sugerida, t.responsavel
""")

display(spark.sql(f"""
SELECT regra_id, classificacao, responsavel, acao_sugerida,
       SUM(linhas) AS linhas, ROUND(SUM(valor_retido_brl), 2) AS valor_retido_brl
FROM {DQ}.vw_quarentena_fato
GROUP BY ALL
ORDER BY regra_id
"""))

# COMMAND ----------

# Intermediários do checkpoint não fazem parte do modelo
for nome in ("fato_etapa1", "fato_etapa2"):
    spark.sql(f"DROP TABLE IF EXISTS {SILVER}.stg_{nome}")