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
# MAGIC | Loja | `ms_silver.dim_loja` + `ms_silver.ref_cidade_uf` + `ms_dq.vw_revalidacao_loja` |
# MAGIC | Território | `ms_silver.territorio_historico` (SCD2 por loja x categoria) |
# MAGIC | Calendário | `ms_silver.dim_calendario` (semana ISO 8601; ano e mês da quinta-feira) |

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
from silver.calendario import CalendarioBuilder
from silver.loja import LojaBuilder
from silver.produto import ProdutoBuilder
from silver.territorio import TerritorioBuilder

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
# MAGIC ## Loja

# COMMAND ----------

loja = LojaBuilder(spark, engine, referencias)
dim_loja = loja.build(spark.table(f"{BRONZE}.dim_loja"))

(dim_loja.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
         .saveAsTable(f"{SILVER}.dim_loja"))
(loja.ref_cidade_uf.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
                   .saveAsTable(f"{SILVER}.ref_cidade_uf"))

display(spark.table(f"{SILVER}.ref_cidade_uf"))

# COMMAND ----------

# Garantias: 1 linha por loja e UF sempre no domínio oficial
dl = spark.table(f"{SILVER}.dim_loja")
linhas, lojas = dl.count(), dl.select("store_id").distinct().count()
fora_dominio = dl.where(~F.col("state").isin(referencias["loja"]["ufs_validas"])).count()
print(f"dim_loja: {linhas} linhas | {lojas} lojas | UF fora do domínio: {fora_dominio}")
assert linhas == lojas, "store_id duplicado na dimensão de loja"
assert fora_dominio == 0, "UF fora do domínio oficial após a LOJ_002"

display(dl.groupBy("dq_status").count().orderBy("dq_status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Território (histórico)

# COMMAND ----------

territorio = TerritorioBuilder(spark, engine)
hist = territorio.build(spark.table(f"{BRONZE}.customer_territory_history"), spark.table(f"{SILVER}.dim_loja"))

(hist.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{SILVER}.territorio_historico"))

# COMMAND ----------

# Garantia: nenhuma sobreposição de vigência (senão o join as-of duplica venda na fato)
from pyspark.sql import Window

th = spark.table(f"{SILVER}.territorio_historico")
w = Window.partitionBy("store_id", "category").orderBy("valid_from")
residual = th.withColumn("_prox", F.lead("valid_from").over(w)).where("_prox IS NOT NULL AND valid_to >= _prox").count()
print(f"territorio_historico: {th.count()} vigências | sobreposições restantes: {residual}")
assert residual == 0, "vigências sobrepostas após a TER_001"

display(th.where("store_id = 'S00009'").orderBy("category", "valid_from"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calendário

# COMMAND ----------

calendario = CalendarioBuilder(spark, engine)
dim_calendario = calendario.build(spark.table(f"{BRONZE}.dim_calendario"))

(dim_calendario.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
               .saveAsTable(f"{SILVER}.dim_calendario"))

# COMMAND ----------

# Garantia: calendário íntegro (CAL_002 / CAL_003 são REJEITADO = bloqueiam o pipeline)
dc = spark.table(f"{SILVER}.dim_calendario")
quebradas = dc.where(F.arrays_overlap("dq_flags", F.array(F.lit("CAL_002"), F.lit("CAL_003")))).count()
print(f"dim_calendario: {dc.count()} semanas | quebradas: {quebradas}")
assert quebradas == 0, "calendário com semana incompleta, lacuna ou year_week inválido"

display(dc.where("size(dq_flags) > 0")
          .select("year_week", "week_start_date", "week_end_date", "year_raw", "year", "month_raw", "month", "dq_flags")
          .orderBy("week_start_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log das regras e gravação

# COMMAND ----------

display(engine.resultados_df())
engine.flush(DQ)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fila de revalidação de cadastro (loja)
# MAGIC Pendências que o pipeline **não corrige** (ou corrige só aqui) e que precisam ser resolvidas na origem.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {DQ}.vw_revalidacao_loja AS
SELECT l.store_id, l.city, l.state, f.regra_id,
       CASE f.regra_id
         WHEN 'LOJ_002' THEN 'UF incoerente com a cidade (corrigida na Silver)'
         WHEN 'LOJ_004' THEN 'Coordenada fora do território brasileiro (IBGE)'
         WHEN 'LOJ_006' THEN 'Território não informado'
         WHEN 'LOJ_007' THEN 'Vendedor não informado'
       END AS problema,
       CASE f.regra_id
         WHEN 'LOJ_002' THEN concat('UF de origem: ', coalesce(l.state_raw, '<vazio>'), ' | cidade: ', l.city)
         WHEN 'LOJ_004' THEN concat('lat=', l.latitude, ' lon=', l.longitude, ' (', l.geo_status, ')')
         ELSE '-'
       END AS evidencia,
       CASE f.regra_id
         WHEN 'LOJ_002' THEN 'Corrigir a UF no cadastro de origem'
         WHEN 'LOJ_004' THEN 'Confirmar lat/lon no cadastro; enviar endereço e CEP'
         WHEN 'LOJ_006' THEN 'Atribuir território no CRM'
         WHEN 'LOJ_007' THEN 'Atribuir vendedor no CRM'
       END AS acao_sugerida
FROM {SILVER}.dim_loja l
LATERAL VIEW explode(l.dq_flags) f AS regra_id
WHERE f.regra_id IN ('LOJ_002', 'LOJ_004', 'LOJ_006', 'LOJ_007')
""")

display(spark.sql(f"SELECT regra_id, problema, count(*) AS lojas FROM {DQ}.vw_revalidacao_loja GROUP BY ALL ORDER BY regra_id"))