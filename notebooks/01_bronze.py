# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze
# MAGIC
# MAGIC Cópia fiel dos arquivos de origem em tabelas Delta.
# MAGIC
# MAGIC - **Todas as colunas como string**: nada é convertido nem descartado aqui.
# MAGIC - **Metadados de rastreio**: arquivo de origem, data do arquivo, hash da linha e id da carga.
# MAGIC - **Reconciliação**: linhas lidas × volume declarado no README do case.
# MAGIC - **Nenhuma correção**: a Bronze é o ponto de reprocessamento. Toda regra nova roda a partir daqui.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("load_id", "manual")

catalog = dbutils.widgets.get("catalog")
load_id = dbutils.widgets.get("load_id")

LANDING = f"/Volumes/{catalog}/ms_raw/landing"
BRONZE = f"{catalog}.ms_bronze"
DQ = f"{catalog}.ms_dq"

print(f"landing={LANDING} | bronze={BRONZE} | load_id={load_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fontes e volumes esperados
# MAGIC Os volumes vêm do README do pacote de dados e viram uma regra de **reconciliação**.
# MAGIC O arquivo de contatos não está na lista de propósito (minimização de dados).

# COMMAND ----------

SOURCES = {
    # tabela bronze               (arquivo,                              linhas esperadas)
    "dim_produto":                ("dim_produto.csv",                    1_235),
    "dim_loja":                   ("dim_loja.csv",                       2_500),
    "dim_calendario":             ("dim_calendario.csv",                    91),
    "customer_territory_history": ("customer_territory_history.csv",     4_736),
    "coverage_provider":          ("coverage_provider.csv",              6_370),
    "fact_provider_a":            ("fact_market_share_provider_a.csv",  68_526),
    "fact_provider_b":            ("fact_market_share_provider_b.csv",  58_526),
}

# COMMAND ----------

import hashlib
from pyspark.sql import DataFrame, functions as F


def sha256_file(path: str) -> str:
    """Checksum do arquivo original: prova de que a entrada não foi alterada."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def clean_name(name: str) -> str:
    """Remove BOM UTF-8 e espaços do nome da coluna (defensivo: não depende do leitor)."""
    return name.replace("﻿", "").strip()


def read_raw(file_name: str) -> DataFrame:
    """Lê o CSV com tudo como string e captura os metadados do arquivo."""
    raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)   # string em tudo: nenhum valor é perdido em silêncio
        .csv(f"{LANDING}/{file_name}")
    )
    data_cols = [F.col(f"`{c}`").alias(clean_name(c)) for c in raw.columns]
    # _metadata é uma coluna oculta das leituras de arquivo: precisa ser selecionada antes de qualquer projeção
    meta_cols = [
        F.col("_metadata.file_name").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_source_modified_at"),
    ]
    return raw.select(*data_cols, *meta_cols)


def add_lineage(df: DataFrame, load_id: str) -> DataFrame:
    """Hash do conteúdo da linha (identifica duplicatas exatas) + id da carga."""
    data_cols = [c for c in df.columns if not c.startswith("_")]
    content = F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<null>")) for c in data_cols])
    return (
        df.withColumn("_row_hash", F.sha2(content, 256))
          .withColumn("_load_id", F.lit(load_id))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga
# MAGIC `overwrite` em cada tabela: rodar de novo **substitui** a carga, nunca duplica (idempotente).

# COMMAND ----------

log_rows = []

for table, (file_name, expected) in SOURCES.items():
    df = add_lineage(read_raw(file_name), load_id)

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{BRONZE}.{table}"))

    rows = spark.table(f"{BRONZE}.{table}").count()
    log_rows.append({
        "load_id": load_id,
        "table_name": f"{BRONZE}.{table}",
        "source_file": file_name,
        "sha256": sha256_file(f"{LANDING}/{file_name}"),
        "expected_rows": expected,
        "loaded_rows": rows,
        "status": "APROVADO" if rows == expected else "APROVADO_COM_ALERTA",
    })
    print(f"{table:<28} {rows:>7} / {expected:>7}  {'OK' if rows == expected else 'DIVERGENTE'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log da carga (reconciliação)
# MAGIC Tabela **append-only**: cada execução acrescenta seu registro. É histórico de auditoria, por isso não sobrescreve.

# COMMAND ----------

log_df = (spark.createDataFrame(log_rows)
          .withColumn("loaded_at", F.current_timestamp()))

(log_df.write.format("delta")
       .mode("append")
       .option("mergeSchema", "true")
       .saveAsTable(f"{DQ}.bronze_load_log"))

display(log_df)

# COMMAND ----------

divergent = [r for r in log_rows if r["status"] != "APROVADO"]
assert not divergent, f"Volumes divergentes do README: {divergent}"
print("Reconciliação OK: todos os volumes batem com o README do case.")