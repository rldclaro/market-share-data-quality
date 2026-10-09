"""Silver · dimensão de produto.

Sequência de regras (a ordem importa):
  PRD_001  product_id único                      -> quarentena
  PRD_004  texto padronizado                     -> corrige (trim/maiúsculas)
  PRD_005  grafia de categoria (de-para)         -> corrige
           hierarquia subcategoria -> categoria  -> DERIVADA dos dados (categoria dominante)
  PRD_006  categoria vazia                       -> corrige pela hierarquia
  PRD_002  EAN único (sobrevivência)             -> alias rejeitado
  PRD_007  conflito com evidência na descrição   -> corrige pela hierarquia
  PRD_008  conflito sem evidência                -> alerta
  PRD_003  EAN fora do padrão                    -> alerta + sugestão (não corrige)
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.engine import DQEngine

ALVO = "dim_produto"


def texto_padronizado(col: str) -> Column:
    return F.upper(F.trim(F.regexp_replace(F.col(col), r"\s+", " ")))


def mapa(d: dict[str, str]) -> Column:
    """dict Python -> map literal do Spark (para usar como de-para dentro de uma expressão)."""
    return F.create_map(*[F.lit(x) for par in d.items() for x in par])


class ProdutoBuilder:
    def __init__(self, spark: SparkSession, engine: DQEngine, referencias: dict):
        self.spark = spark
        self.engine = engine
        self.ref = referencias["produto"]
        self.hierarquia: DataFrame | None = None  # de-para derivado, com evidência (vira tabela)

    # ------------------------------------------------------------------ hierarquia
    def derivar_hierarquia(self, df: DataFrame) -> dict[str, str]:
        """Categoria dominante por subcategoria. Só entram linhas com evidência >= limite."""
        contagem = (df.where(F.col("category").isNotNull() & (F.col("category") != ""))
                      .groupBy("subcategory", "category").agg(F.count("*").alias("produtos")))
        w = Window.partitionBy("subcategory")
        self.hierarquia = (
            contagem
            .withColumn("total_subcategoria", F.sum("produtos").over(w))
            .withColumn("posicao", F.row_number().over(w.orderBy(F.desc("produtos"), "category")))
            .where("posicao = 1")
            .withColumn("pct_evidencia", F.round(F.col("produtos") / F.col("total_subcategoria"), 4))
            .withColumn("status", F.when(F.col("pct_evidencia") >= self.ref["hierarquia_evidencia_minima"],
                                         "APROVADO").otherwise("REVISAR"))
            .select(F.col("subcategory").alias("subcategoria"), F.col("category").alias("categoria_oficial"),
                    F.col("produtos").alias("produtos_que_confirmam"), "total_subcategoria",
                    "pct_evidencia", "status")
        )
        linhas = self.hierarquia.where("status = 'APROVADO'").collect()   # 5 linhas: cabe no driver
        return {r["subcategoria"]: r["categoria_oficial"] for r in linhas}

    # ------------------------------------------------------------------ pipeline
    def build(self, bronze: DataFrame) -> DataFrame:
        e = self.engine
        chave = ["product_id", "ean"]
        df = bronze.drop("_load_id")

        # PRD_001 — product_id único
        df = e.quarantine(df, "PRD_001", F.count("*").over(Window.partitionBy("product_id")) > 1,
                          alvo=ALVO, chave=chave, col_valor=None)

        # PRD_004 — texto padronizado
        cols = self.ref["colunas_texto"]
        diferente = F.lit(False)
        for c in cols:
            diferente = diferente | (F.col(c) != texto_padronizado(c))
        df = e.correct(df, "PRD_004", diferente, {c: texto_padronizado(c) for c in cols},
                       alvo=ALVO, chave=chave, col_valor=None)

        # PRD_005 — grafia da categoria (de-para de sinônimos)
        sinonimos = mapa(self.ref["categoria_sinonimos"])
        df = e.correct(df, "PRD_005", sinonimos[F.col("category")].isNotNull(),
                       {"category": sinonimos[F.col("category")]}, alvo=ALVO, chave=chave, col_valor=None)

        # Hierarquia derivada dos dados (já com grafia padronizada)
        hierarquia = mapa(self.derivar_hierarquia(df))
        esperada = hierarquia[F.col("subcategory")]

        # PRD_006 — categoria vazia -> hierarquia
        vazia = F.col("category").isNull() | (F.col("category") == "")
        df = e.correct(df, "PRD_006", vazia & esperada.isNotNull(), {"category": esperada},
                       alvo=ALVO, chave=chave, col_valor=None)

        # PRD_002 — EAN único: sobrevivência determinística
        coerente = (F.col("category") == esperada).cast("int")
        canonico = F.col("product_id").rlike(r"^P\d{5}$").cast("int")
        w_ean = Window.partitionBy("ean")
        w_rank = w_ean.orderBy(coerente.desc(), canonico.desc(), F.col("product_id"))
        df = (df.withColumn("_ordem", F.row_number().over(w_rank))
                .withColumn("_sobrevivente", F.first("product_id").over(
                    w_rank.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
                .withColumn("alias_product_ids", F.array_sort(F.array_remove(
                    F.collect_set("product_id").over(w_ean), F.col("product_id")))))
        df = e.quarantine(df, "PRD_002", F.col("_ordem") > 1, alvo=ALVO, chave=chave, col_valor=None,
                          motivo=F.concat(F.lit("alias de EAN duplicado; sobrevivente = "), F.col("_sobrevivente")))
        df = df.drop("_ordem", "_sobrevivente")

        # PRD_007 / PRD_008 — conflito categoria x subcategoria
        conflito = esperada.isNotNull() & (F.col("category") != esperada)
        evidencia = F.instr(F.col("product_description"), F.col("subcategory")) > 0
        df = e.correct(df, "PRD_007", conflito & evidencia, {"category": esperada},
                       alvo=ALVO, chave=chave, col_valor=None,
                       detalhes="evidência: a descrição contém a subcategoria")
        df = e.check(df, "PRD_008", conflito, alvo=ALVO, col_valor=None)

        # PRD_003 — EAN fora do padrão: alerta + sugestão, sem alterar o código
        malformado = ~F.col("ean").rlike(r"^\d{13}$")
        df = df.withColumn("ean_sugerido", F.when(malformado, F.concat(
            F.lit("7891000"), F.lpad(F.regexp_extract("product_id", r"(\d+)$", 1), 6, "0"))))
        df = e.check(df, "PRD_003", malformado, alvo=ALVO, col_valor=None)

        # Tipos de negócio (a Bronze é toda string)
        fator_kg_l = F.when(F.col("package_unit").isin("G", "ML"), F.lit(0.001)).otherwise(F.lit(1.0))
        df = (df.withColumn("package_size", F.col("package_size").cast("int"))
                .withColumn("package_kg_l", F.round(F.col("package_size") * fator_kg_l, 6))
                .withColumn("valid_from", F.to_date("valid_from"))
                .withColumn("valid_to", F.to_date("valid_to")))
        return e.com_status(df)
