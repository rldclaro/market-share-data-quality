# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Evidências
# MAGIC
# MAGIC Última task do job:
# MAGIC 1. **Pente fino** (`sql/validacao/00_pente_fino.sql`, 41 verificações). Qualquer `FALHA` **falha o job**.
# MAGIC 2. **Exporta** as evidências (CSVs) e **gera os gráficos** dos insights a partir das tabelas publicadas.
# MAGIC 3. Grava tudo no Volume `ms_dq.evidencias`. Para trazer ao repositório: `./scripts/baixar_evidencias.sh`.

# COMMAND ----------

import os
import shutil
import sys
import tempfile

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("root", "")


def raiz_do_projeto(inicio: str) -> str:
    """Sobe as pastas até achar o databricks.yml."""
    p = inicio
    while p != "/" and not os.path.exists(os.path.join(p, "databricks.yml")):
        p = os.path.dirname(p)
    return p


catalog = dbutils.widgets.get("catalog")
root = dbutils.widgets.get("root") or raiz_do_projeto(os.getcwd())
sys.path.insert(0, f"{root}/src")

from relatorios.evidencias import exportar_csvs, gerar_graficos, pente_fino


def tabela(nome: str) -> str:
    """Nome lógico ("ms_gold.market_share") -> nome no Unity Catalog."""
    return f"{catalog}.{nome}"


spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.ms_dq.evidencias")
VOLUME = f"/Volumes/{catalog}/ms_dq/evidencias"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pente fino

# COMMAND ----------

with open(f"{root}/sql/validacao/00_pente_fino.sql", encoding="utf-8") as fh:
    resultado = pente_fino(spark, fh.read(), catalog).toPandas()

display(resultado)
falhas = resultado[resultado.status != "OK"]
print(f"Pente fino: {len(resultado) - len(falhas)}/{len(resultado)} OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exportações e gráficos
# MAGIC Gerados numa pasta temporária e copiados para o Volume (escrita sequencial, compatível com Volumes).

# COMMAND ----------

tmp = tempfile.mkdtemp()
resultado.to_csv(os.path.join(tmp, "pente_fino.csv"), index=False)
linhas = exportar_csvs(spark, tabela, tmp)
graficos = gerar_graficos(spark, tabela, os.path.join(tmp, "graficos"))

for pasta, _, nomes in os.walk(tmp):
    relativo = os.path.relpath(pasta, tmp)
    os.makedirs(os.path.join(VOLUME, relativo), exist_ok=True)
    for nome in nomes:
        shutil.copy(os.path.join(pasta, nome), os.path.join(VOLUME, relativo, nome))

print("CSVs:", linhas)
print("Gráficos:", [os.path.basename(g) for g in graficos])
display(dbutils.fs.ls(VOLUME))

# COMMAND ----------

# Portão final: as evidências são gravadas antes (para investigar), e o job falha se o pente fino falhou
assert falhas.empty, f"Pente fino com falha: {falhas.verificacao.tolist()}"
