"""Silver · fato de vendas (fornecedores A + B).

A ordem importa: primeiro o que RETIRA registros (tipo inválido, duplicata, conflito, órfão),
depois o que CORRIGE, por último o que só ALERTA. Assim nenhuma correção é aplicada a um
registro que vai para a quarentena, e os alertas são calculados só sobre o que segue no fluxo.

  ING_002/003  schema de cada fornecedor (obrigatórias / drift)   -> interrompe / alerta
  ING_005      tipos válidos                                     -> quarentena
  ING_004      colunas legadas = padronizadas                    -> alerta
  FCT_001      duplicata exata                                   -> rejeita as cópias
  FCT_002      versões conflitantes no mesmo fornecedor          -> quarentena (todas)
  FCT_003      mesma chave em A e B                              -> quarentena (as duas)
  FCT_006/004/005  semana, loja e produto existem nas dimensões  -> quarentena
  FCT_007      negativo com sinal invertido comprovado           -> corrige
  FCT_008      negativo sem prova                                -> quarentena
  FCT_009      preço unitário fora da faixa do produto           -> quarentena
  FCT_010      pico extremo curto na série                       -> quarentena
  FCT_011/012  evento sustentado / queda na série                -> alerta
  FCT_013..016 consistência e completude                        -> alerta / informativo
  FCT_017      semana aberta na ingestão                         -> alerta (MS não oficial)
  FCT_018      território as-of (histórico -> dim_loja -> SEM_TERRITORIO) -> alerta
  FCT_019      competitor_flag x cadastro                        -> alerta
  FCT_020      volume x embalagem (dataset)                      -> alerta

Joins com a fato: F.broadcast() no lado menor — calendário (91), loja (2.500), produto (1.200),
histórico de território (4.736) e mediana por produto (1.200). A dimensão vai inteira para cada
executor e a fato não sofre shuffle; no join as-of de território (chave + intervalo de datas) o
broadcast evita redistribuir a fato pela chave.

Exceção consciente: os agregados por SÉRIE (fornecedor x loja x produto) NÃO levam broadcast.
São ~119 mil séries para ~125 mil linhas — praticamente o tamanho da fato, e crescem junto com
ela. Forçar broadcast aqui não reduz trabalho e, em volume de produção, estoura a memória do
driver. O AQE escolhe a estratégia pelo tamanho real (neste volume ele mesmo faz broadcast).

Union: unionByName(allowMissingColumns=True) — casa as colunas pelo NOME, não pela posição
(A e B têm ordem e colunas diferentes; o que só existe em um fornecedor vira NULL no outro).

Serverless não permite cache(): `checkpoint` grava o intermediário em Delta e relê, cortando a
linhagem (cada regra conta falhas com uma agregação; sem o corte, cada uma recalcularia tudo).
"""
from __future__ import annotations

from functools import reduce
from typing import Callable

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.engine import DQEngine

ALVO = "fato_vendas"
CHAVE_NEGOCIO = ["year_week", "store_id", "ean"]
CHAVE = ["provider", *CHAVE_NEGOCIO, "_row_hash"]
SERIE = ["provider", "store_id", "product_id"]
META = ["_source_file", "_source_modified_at", "_row_hash"]

Checkpoint = Callable[[DataFrame, str], DataFrame]


class SchemaError(RuntimeError):
    pass


class FatoBuilder:
    def __init__(self, spark: SparkSession, engine: DQEngine, referencias: dict, checkpoint: Checkpoint):
        self.spark = spark
        self.engine = engine
        self.ref = referencias["fato"]
        self.checkpoint = checkpoint

    # ------------------------------------------------------------------ 1. harmonização (union)
    def harmonizar(self, bronze: dict[str, DataFrame]) -> DataFrame:
        """Mapeia cada fornecedor para o schema canônico e une com unionByName."""
        e, frames = self.engine, []
        for provider, spec in self.ref["fornecedores"].items():
            df = bronze[spec["tabela"]]
            colunas = [c for c in df.columns if not c.startswith("_")]
            obrig, opc, leg = spec["obrigatorias"], spec.get("opcionais", {}), spec.get("legado", {})

            faltando = [c for c in obrig.values() if c not in colunas]
            e.record("ING_002", f"fornecedor_{provider}", avaliados=len(obrig), falhas=len(faltando),
                     detalhes=f"faltando={faltando}")
            if faltando:
                raise SchemaError(f"Fornecedor {provider}: colunas obrigatórias ausentes {faltando}")

            conhecidas = set(obrig.values()) | set(opc.values()) | set(leg.keys())
            novas = [c for c in colunas if c not in conhecidas]
            e.record("ING_003", f"fornecedor_{provider}", avaliados=len(colunas), falhas=len(novas),
                     detalhes=f"não mapeadas={novas}")

            sel = [F.lit(provider).alias("provider")]
            sel += [F.col(origem).alias(canon) for canon, origem in {**obrig, **opc}.items()]
            sel += [F.col(origem).alias(f"legado_{canon}") for origem, canon in leg.items()]
            frames.append(df.select(*sel, *META))
        return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)

    # ------------------------------------------------------------------ 2. tipagem
    def tipar(self, df: DataFrame) -> DataFrame:
        """try_cast: o que não converte vira NULL e vai para quarentena com o texto original.

        DECIMAL para dinheiro e volume: soma exata e independente da ordem das partições
        (com double, duas execuções podem divergir na última casa e quebrar a idempotência).
        """
        tipos = {"units": "bigint", "sales_value_brl": "decimal(18,2)", "sold_volume": "decimal(18,3)",
                 "average_price": "decimal(18,4)",
                 "legado_sales_value_brl": "decimal(18,2)", "legado_sold_volume": "decimal(18,3)"}
        for c, t in tipos.items():
            df = df.withColumn(f"_t_{c}", F.expr(f"try_cast({c} AS {t})"))
        df = df.withColumn("_t_ingestion_ts", F.expr("try_to_timestamp(ingestion_ts)"))

        invalido = ~F.col("year_week").rlike(r"^\d{4}-\d{2}$") | F.col("_t_ingestion_ts").isNull()
        for c in ("units", "sales_value_brl", "sold_volume"):
            invalido = invalido | F.col(f"_t_{c}").isNull()
        invalido = invalido | (F.col("average_price").isNotNull() & F.col("_t_average_price").isNull())
        df = self.engine.quarantine(df, "ING_005", invalido, alvo=ALVO, chave=CHAVE, col_valor=None)

        for c in [*tipos, "ingestion_ts"]:
            df = df.withColumn(c, F.col(f"_t_{c}")).drop(f"_t_{c}")
        return df

    # ------------------------------------------------------------------ 3. faixa de preço
    def _faixa_preco(self, df: DataFrame) -> DataFrame:
        """Mediana do preço unitário: LOCAL (série loja x produto) ou, sem histórico, do PRODUTO."""
        fp, min_sem = self.ref["faixa_preco"], self.ref["serie"]["min_semanas"]
        valido = (F.col("units") > 0) & (F.col("sales_value_brl") > 0)
        base = df.where(valido).withColumn("_pu", F.col("sales_value_brl") / F.col("units"))
        prod = base.groupBy("product_id").agg(F.median("_pu").alias("_med_prod"), F.count("*").alias("_n_prod"))
        local = base.groupBy(*SERIE).agg(F.median("_pu").alias("_med_local"),
                                         F.countDistinct("year_week").alias("_n_local"))
        df = (df.join(F.broadcast(prod), "product_id", "left").join(local, SERIE, "left")   # série: sem broadcast (ver docstring)
                .withColumn("unit_price", F.when(valido, F.round(F.col("sales_value_brl") / F.col("units"), 4))))
        usa_local = F.coalesce(F.col("_n_local"), F.lit(0)) >= min_sem
        usa_prod = ~usa_local & (F.coalesce(F.col("_n_prod"), F.lit(0)) >= fp["min_obs_produto"])
        ref = F.when(usa_local, F.col("_med_local")).when(usa_prod, F.col("_med_prod"))
        k = F.when(usa_local, F.lit(fp["fator_local"])).otherwise(F.lit(fp["fator_produto"]))
        return (df.withColumn("price_baseline", F.when(usa_local, "LOCAL").when(usa_prod, "PRODUTO"))
                  .withColumn("price_band_low", F.round(ref / k, 4))
                  .withColumn("price_band_high", F.round(ref * k, 4))
                  .withColumn("_fora_faixa", valido & ref.isNotNull()
                              & ((F.col("unit_price") < F.col("price_band_low"))
                                 | (F.col("unit_price") > F.col("price_band_high"))))
                  .drop("_med_prod", "_n_prod", "_med_local", "_n_local"))

    # ------------------------------------------------------------------ 4. anomalias por série
    def _anomalias(self, df: DataFrame) -> DataFrame:
        """Baseline LOCAL (mediana + MAD das unidades) e duração do desvio (gaps-and-islands).

        Mediana/MAD e não média/desvio: o próprio evento contamina média e desvio; a mediana
        tolera até ~50% de pontos anômalos na série.
        """
        s = self.ref["serie"]
        base = df.groupBy(*SERIE).agg(F.countDistinct("year_week").alias("series_weeks"),
                                      F.median("units").alias("series_median_units"))
        mad = (df.join(base, SERIE).where(F.col("series_weeks") >= s["min_semanas"])
                 .groupBy(*SERIE)
                 .agg(F.median(F.abs(F.col("units") - F.col("series_median_units"))).alias("series_mad_units")))
        df = df.join(base, SERIE, "left").join(mad, SERIE, "left")   # série: sem broadcast (ver docstring)

        elegivel = (F.col("series_weeks") >= s["min_semanas"]) & (F.col("series_median_units") > 0)
        razao = F.try_divide(F.col("units"), F.col("series_median_units"))       # ANSI: sem divisão por zero
        # MAD = 0 (série constante): escala mínima de 1 unidade, senão um pico nunca seria detectado
        escala = F.when(F.col("series_mad_units") > 0, 1.4826 * F.col("series_mad_units")).otherwise(F.lit(1.0))
        z = F.try_divide(F.col("units") - F.col("series_median_units"), escala)
        df = (df.withColumn("series_ratio", F.when(elegivel, F.round(razao, 3)))
                .withColumn("series_robust_z", F.when(elegivel, F.round(z, 2)))
                .withColumn("_alta", elegivel & (F.coalesce(z, F.lit(0.0)) > s["z_robusto"])
                            & (F.coalesce(razao, F.lit(0.0)) >= s["razao_alta"]))
                .withColumn("_baixa", elegivel & (F.coalesce(z, F.lit(0.0)) < -s["z_robusto"])
                            & (F.coalesce(razao, F.lit(1.0)) <= s["razao_baixa"])))

        # duração: semanas consecutivas em alta formam uma "ilha" (week_seq - row_number constante)
        w = Window.partitionBy(*SERIE, "_alta").orderBy("week_seq")
        df = df.withColumn("_ilha", F.col("week_seq") - F.row_number().over(w))
        df = df.withColumn("anomaly_run_weeks",
                           F.when(F.col("_alta"), F.count("*").over(Window.partitionBy(*SERIE, "_alta", "_ilha"))))
        classe = (F.when(F.col("_alta") & (F.col("series_ratio") >= s["razao_extrema"])
                         & (F.col("anomaly_run_weeks") <= s["max_semanas_pico"]), "POSSIVEL_ERRO")
                   .when(F.col("_alta"), "EVENTO_ATIPICO")
                   .when(F.col("_baixa"), "QUEDA_ATIPICA")
                   .when(elegivel, "VARIACAO_ACEITAVEL"))
        return df.withColumn("anomaly_class", classe).drop("_alta", "_baixa", "_ilha")

    # ------------------------------------------------------------------ 5. pipeline
    def build(self, canonica: DataFrame, produto: DataFrame, loja: DataFrame,
              territorio: DataFrame, calendario: DataFrame) -> DataFrame:
        e, ref = self.engine, self.ref
        df = self.tipar(canonica)

        # ING_004 — legado x padronizado
        df = e.check(df, "ING_004", (F.col("legado_sales_value_brl") != F.col("sales_value_brl"))
                     | (F.col("legado_sold_volume") != F.col("sold_volume")), alvo=ALVO)

        # FCT_001 — duplicata exata: fica a 1ª ocorrência, as cópias são rejeitadas
        ocorrencia = F.row_number().over(Window.partitionBy("provider", "_row_hash").orderBy("_source_modified_at"))
        df = df.withColumn("_ocorrencia", ocorrencia)
        df = e.quarantine(df, "FCT_001", F.col("_ocorrencia") > 1, alvo=ALVO, chave=CHAVE,
                          motivo=F.concat(F.lit("cópia "), F.col("_ocorrencia").cast("string"),
                                          F.lit(" do hash "), F.substring("_row_hash", 1, 16)))
        df = df.drop("_ocorrencia")

        # FCT_002 — mesma chave, mesmo fornecedor, valores diferentes: todas as versões
        df = e.quarantine(df, "FCT_002", F.count("*").over(Window.partitionBy("provider", *CHAVE_NEGOCIO)) > 1,
                          alvo=ALVO, chave=CHAVE,
                          motivo=F.concat(F.lit("versões conflitantes; arquivo="), F.col("source_file")))

        # FCT_003 — mesma chave em A e B
        fornecedores = F.size(F.collect_set("provider").over(Window.partitionBy(*CHAVE_NEGOCIO)))
        df = e.quarantine(df, "FCT_003", fornecedores > 1, alvo=ALVO, chave=CHAVE,
                          motivo=F.lit("mesma semana x loja x EAN informada por A e B"))

        # FCT_006 / FCT_004 / FCT_005 — integridade com as dimensões
        cal = calendario.select("year_week", "week_start_date", "week_end_date", "year", "month",
                                F.floor(F.datediff("week_start_date", F.lit("1970-01-05")) / 7).cast("int").alias("week_seq"),
                                F.lit(True).alias("_cal"))
        df = df.join(F.broadcast(cal), "year_week", "left")
        df = e.quarantine(df, "FCT_006", F.col("_cal").isNull(), alvo=ALVO, chave=CHAVE).drop("_cal")

        lj = loja.select("store_id", "retailer_name", "channel", "city", "state",
                         F.col("territory_id").alias("_territorio_loja"), F.lit(True).alias("_loja"))
        df = df.join(F.broadcast(lj), "store_id", "left")
        df = e.quarantine(df, "FCT_004", F.col("_loja").isNull(), alvo=ALVO, chave=CHAVE,
                          motivo=F.concat(F.lit("loja inexistente na dim_loja: "), F.col("store_id"))).drop("_loja")

        pr = produto.select("ean", "product_id", "brand", "manufacturer", "category", "subcategory",
                            "is_own_product", "package_kg_l", F.lit(True).alias("_prod"))
        df = df.join(F.broadcast(pr), "ean", "left")
        df = e.quarantine(df, "FCT_005", F.col("_prod").isNull(), alvo=ALVO, chave=CHAVE,
                          motivo=F.concat(F.lit("EAN inexistente na dim_produto: "), F.col("ean"))).drop("_prod")

        # FCT_007 — sinal invertido comprovado; FCT_008 — negativo sem prova
        tol, tol_abs = ref["tolerancia_preco_pct"], ref["tolerancia_preco_abs"]
        comprovado = ((F.col("sales_value_brl") < 0) & (F.col("units") > 0) & F.col("average_price").isNotNull()
                      & (F.abs(F.col("units") * F.col("average_price") + F.col("sales_value_brl"))
                         <= tol * F.abs(F.col("sales_value_brl")) + tol_abs))
        df = e.correct(df, "FCT_007", comprovado, {"sales_value_brl": F.abs(F.col("sales_value_brl"))},
                       alvo=ALVO, chave=CHAVE, detalhes="prova: unidades x preço médio = -valor")
        df = e.quarantine(df, "FCT_008", F.col("sales_value_brl") < 0, alvo=ALVO, chave=CHAVE,
                          motivo=F.lit("valor negativo sem prova de sinal invertido"))

        df = self.checkpoint(df, "fato_etapa1")

        # FCT_009 — preço unitário fora da faixa
        df = self._faixa_preco(df)
        df = e.quarantine(df, "FCT_009", F.col("_fora_faixa"), alvo=ALVO, chave=CHAVE,
                          motivo=F.format_string("preço unitário %.2f fora da faixa %s [%.2f, %.2f]",
                                                 F.col("unit_price").cast("double"), F.col("price_baseline"),
                                                 F.col("price_band_low").cast("double"),
                                                 F.col("price_band_high").cast("double"))).drop("_fora_faixa")

        # FCT_010 / 011 / 012 — anomalias por série
        df = self._anomalias(df)
        df = self.checkpoint(df, "fato_etapa2")
        df = e.quarantine(df, "FCT_010", F.col("anomaly_class") == "POSSIVEL_ERRO", alvo=ALVO, chave=CHAVE,
                          motivo=F.format_string("pico de %.1fx a mediana da série por %d semana(s)",
                                                 F.col("series_ratio").cast("double"), F.col("anomaly_run_weeks")))
        com_baseline = F.col("anomaly_class").isNotNull()
        df = e.check(df, "FCT_011", F.col("anomaly_class") == "EVENTO_ATIPICO", alvo=ALVO, avaliado=com_baseline)
        df = e.check(df, "FCT_012", F.col("anomaly_class") == "QUEDA_ATIPICA", alvo=ALVO, avaliado=com_baseline)

        # FCT_013..016 — consistência e completude
        df = e.check(df, "FCT_013",
                     F.abs(F.col("units") * F.col("average_price") - F.col("sales_value_brl"))
                     > tol * F.col("sales_value_brl") + tol_abs,
                     alvo=ALVO, avaliado=F.col("average_price").isNotNull() & (F.col("units") > 0))
        df = e.check(df, "FCT_014", (F.col("units") == 0) & (F.col("sales_value_brl") > 0), alvo=ALVO)
        df = e.check(df, "FCT_015", (F.col("sold_volume") == 0) & (F.col("units") > 0), alvo=ALVO)
        df = e.check(df, "FCT_016", (F.col("units") == 0) & (F.col("sold_volume") == 0)
                     & (F.col("sales_value_brl") == 0), alvo=ALVO)

        # FCT_017 — semana ainda aberta quando o arquivo foi gerado
        fim_semana = F.to_timestamp(F.date_add("week_end_date", 1))
        df = df.withColumn("week_open_at_ingestion", F.col("ingestion_ts") < fim_semana)
        df = e.check(df, "FCT_017", F.col("week_open_at_ingestion"), alvo=ALVO)

        # FCT_018 — território vigente NA SEMANA DA VENDA (join as-of com o histórico SCD2)
        df = self._territorio(df, territorio)
        df = e.check(df, "FCT_018", F.col("territory_id") == ref["sem_territorio"], alvo=ALVO)

        # FCT_019 — competitor_flag (só B) x cadastro
        df = e.check(df, "FCT_019", (F.col("competitor_flag") == "Y") != (F.col("is_own_product") == "N"),
                     alvo=ALVO, avaliado=F.col("competitor_flag").isNotNull())

        # FCT_020 — volume x unidades x embalagem (regra de dataset)
        esperado = F.col("units") * F.col("package_kg_l")
        r = df.agg(F.count("*").alias("n"),
                   F.sum((F.abs(F.col("sold_volume") - esperado) > 0.01 * esperado + 1e-6).cast("int")).alias("fl")).first()
        e.record("FCT_020", ALVO, avaliados=r["n"], falhas=r["fl"] or 0,
                 detalhes="volume não acompanha a embalagem; unidade adotada: kg (rótulo qty_kg do fornecedor B)")

        df = df.withColumnRenamed("sold_volume", "sold_volume_kg")
        return e.com_status(df)

    def _territorio(self, df: DataFrame, territorio: DataFrame) -> DataFrame:
        """Prioridade: histórico vigente (loja x categoria) -> dim_loja -> SEM_TERRITORIO.

        Data de referência da semana = quinta-feira (mesma regra ISO do calendário): a semana
        2026-01 (29/12 a 04/01) tem 4 dias em 2026, então usa a vigência que começa em 2026-01-01.
        """
        h = territorio.select(F.col("store_id").alias("_h_loja"), F.col("category").alias("_h_cat"),
                              F.col("valid_from").alias("_h_de"), F.col("valid_to").alias("_h_ate"),
                              F.col("territory_id").alias("_h_territorio"), F.col("seller_id").alias("seller_id"))
        cond = ((F.col("store_id") == F.col("_h_loja")) & (F.col("category") == F.col("_h_cat"))
                & F.date_add(F.col("week_start_date"), 3).between(F.col("_h_de"), F.col("_h_ate")))
        df = df.join(F.broadcast(h), cond, "left")
        df = (df.withColumn("territory_source",
                            F.when(F.col("_h_territorio").isNotNull(), "HISTORICO")
                             .when(F.col("_territorio_loja").isNotNull(), "DIM_LOJA")
                             .otherwise("SEM_TERRITORIO"))
                .withColumn("territory_id", F.coalesce(F.col("_h_territorio"), F.col("_territorio_loja"),
                                                       F.lit(self.ref["sem_territorio"]))))
        return df.drop("_h_loja", "_h_cat", "_h_de", "_h_ate", "_h_territorio", "_territorio_loja")
