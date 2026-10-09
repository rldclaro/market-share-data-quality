"""Regras de negócio críticas, com casos de borda: o que corrige, o que alerta, o que retira."""
import datetime as dt

from pyspark.sql import functions as F

from conftest import resultado

D = dt.date


# ------------------------------------------------------------------ loja
def _loja(spark, linhas):
    cols = ("store_id string, store_name string, cnpj string, retailer_name string, channel string, city string, "
            "state string, territory_id string, seller_id string, latitude string, longitude string, "
            "active_flag string, _source_file string, _load_id string")
    return spark.createDataFrame(linhas, cols)


def test_loja_uf_pela_cidade_cnpj_e_coordenada_sem_alterar(spark, engine, referencias):
    from silver.loja import LojaBuilder
    base = ["LOJA", "10000000000001", "REDE 01", "SUPER", "CURITIBA", "PR", "T1", "V1", "-25.4", "-49.2", "Y", "f", "x"]
    linhas = [tuple([f"S{i:05d}"] + base) for i in range(1, 11)]                            # 10 lojas PR
    linhas.append(("S00099", "L", "10.000.000/0000-99", "REDE 01", "SUPER", "CURITIBA", "São Paulo",
                   "T1", "V1", "-42.9", "-20.7", "Y", "f", "x"))                            # UF errada, máscara, lat/lon invertidas
    out = {r.store_id: r for r in LojaBuilder(spark, engine, referencias).build(_loja(spark, linhas)).collect()}
    s = out["S00099"]
    assert (s.state, s.state_raw) == ("PR", "São Paulo")           # LOJ_002 corrige pela cidade
    assert (s.cnpj, s.cnpj_raw) == ("10000000000099", "10.000.000/0000-99")  # LOJ_003
    assert (s.latitude, s.longitude) == (-42.9, -20.7)             # LOJ_004 NÃO altera
    assert s.geo_status == "REVALIDAR_INVERSAO" and "LOJ_004" in s.dq_flags


# ------------------------------------------------------------------ território
def test_territorio_fecha_vigencia_sobreposta_na_vespera(spark, engine):
    from silver.territorio import TerritorioBuilder
    hist = spark.createDataFrame([
        ("S1", "NUTRICAO", "T010", "V1", "2025-01-01", "9999-12-31", "CRM", "f", "x"),
        ("S1", "NUTRICAO", "T022", "V2", "2026-01-01", "9999-12-31", "MANUAL", "f", "x"),
        ("S1", "CULINARIOS", "T010", "V1", "2025-01-01", "9999-12-31", "CRM", "f", "x"),
    ], "store_id string, category string, territory_id string, seller_id string, valid_from string, "
       "valid_to string, assignment_source string, _source_file string, _load_id string")
    lojas = spark.createDataFrame([("S1",)], "store_id string")
    out = {(r.category, r.territory_id): r for r in TerritorioBuilder(spark, engine).build(hist, lojas).collect()}
    assert out[("NUTRICAO", "T010")].valid_to == D(2025, 12, 31)       # fechada na véspera
    assert out[("NUTRICAO", "T010")].valid_to_raw == D(9999, 12, 31)   # original preservado
    assert out[("NUTRICAO", "T022")].valid_to == D(9999, 12, 31)       # a nova vale
    assert out[("CULINARIOS", "T010")].valid_to == D(9999, 12, 31)     # outra categoria intacta
    assert resultado(engine, "TER_001").falhas == 1


# ------------------------------------------------------------------ calendário
def test_calendario_ano_e_mes_iso_pela_quinta_feira(spark, engine):
    from silver.calendario import CalendarioBuilder
    cal = spark.createDataFrame([
        ("2025-12-22", "2025", "12", "52", "2025-52", "2025-12-22", "2025-12-28", "f", "x"),
        ("2025-12-29", "2025", "12", "1", "2026-01", "2025-12-29", "2026-01-04", "f", "x"),   # vira 2026 / 1
    ], "date string, year string, month string, week string, year_week string, week_start_date string, "
       "week_end_date string, _source_file string, _load_id string")
    out = {r.year_week: r for r in CalendarioBuilder(spark, engine).build(cal).collect()}
    assert (out["2026-01"].year, out["2026-01"].month) == (2026, 1)
    assert (out["2026-01"].year_raw, out["2026-01"].month_raw) == (2025, 12)
    assert (out["2025-52"].year, out["2025-52"].month) == (2025, 12)
    assert resultado(engine, "CAL_002").falhas == 0


# ------------------------------------------------------------------ fato
def test_fato_negativo_so_corrige_com_prova(spark, engine, referencias):
    from silver.fato import FatoBuilder
    df = spark.createDataFrame([
        ("A", "2025-02", "S1", "E1", "10", "-100.00", "5.0", "10.00"),   # 10 x 10,00 = 100 -> prova: corrige
        ("A", "2025-02", "S1", "E2", "10", "-100.00", "5.0", "3.00"),    # 10 x 3,00 != 100 -> sem prova
        ("B", "2025-02", "S1", "E3", "10", "-100.00", "5.0", None),      # B não tem preço médio -> sem prova
    ], "provider string, year_week string, store_id string, ean string, units string, "
       "sales_value_brl string, sold_volume string, average_price string")
    df = (df.withColumn("units", F.col("units").cast("bigint"))
            .withColumn("sales_value_brl", F.col("sales_value_brl").cast("decimal(18,2)"))
            .withColumn("average_price", F.col("average_price").cast("decimal(18,4)"))
            .withColumn("_row_hash", F.col("ean")))
    fb = FatoBuilder(spark, engine, referencias, checkpoint=lambda d, n: d)
    tol, tol_abs = fb.ref["tolerancia_preco_pct"], fb.ref["tolerancia_preco_abs"]
    prova = ((F.col("sales_value_brl") < 0) & (F.col("units") > 0) & F.col("average_price").isNotNull()
             & (F.abs(F.col("units") * F.col("average_price") + F.col("sales_value_brl"))
                <= tol * F.abs(F.col("sales_value_brl")) + tol_abs))
    chave = ["provider", "year_week", "store_id", "ean", "_row_hash"]
    df = engine.correct(df, "FCT_007", prova, {"sales_value_brl": F.abs(F.col("sales_value_brl"))},
                        alvo="fato_vendas", chave=chave)
    df = engine.quarantine(df, "FCT_008", F.col("sales_value_brl") < 0, alvo="fato_vendas", chave=chave)
    fica = {r.ean: float(r.sales_value_brl) for r in df.collect()}
    assert fica == {"E1": 100.0}
    assert resultado(engine, "FCT_008").falhas == 2


def test_fato_pico_curto_vira_erro_e_sustentado_vira_evento(spark, engine, referencias):
    from silver.fato import FatoBuilder
    fb = FatoBuilder(spark, engine, referencias, checkpoint=lambda d, n: d)
    linhas = []
    for semana in range(1, 41):                                  # 40 semanas, mediana = 10 unidades
        u_pico = 300 if semana == 20 else 10                     # 1 semana a 30x -> POSSIVEL_ERRO
        u_evento = 60 if 25 <= semana <= 33 else 10              # 9 semanas a 6x -> EVENTO_ATIPICO
        linhas += [("A", "S1", "P1", semana, u_pico), ("A", "S2", "P1", semana, u_evento)]
    df = spark.createDataFrame(linhas, "provider string, store_id string, product_id string, week_seq int, units bigint")
    df = df.withColumn("year_week", F.col("week_seq").cast("string"))
    out = fb._anomalias(df)
    classe = {(r.store_id, r.week_seq): r.anomaly_class for r in out.collect()}
    assert classe[("S1", 20)] == "POSSIVEL_ERRO"
    assert classe[("S2", 28)] == "EVENTO_ATIPICO"
    assert classe[("S1", 5)] == "VARIACAO_ACEITAVEL"


def test_unionbyname_casa_colunas_pelo_nome(spark, engine, referencias):
    from silver.fato import FatoBuilder
    a = spark.createDataFrame([("2025-02", "S1", "E1", "1", "10.0", "1.0", "f.csv", "2026-10-01T00:00:00", "10.0",
                                "10.0", "1.0", "a", None, "h1")],
                              "week string, store_id string, ean string, units string, sales_value_brl string, "
                              "sold_volume string, source_file string, ingestion_timestamp string, average_price string, "
                              "sales_value string, sales_volume string, _source_file string, _source_modified_at timestamp, _row_hash string")
    b = spark.createDataFrame([("S2", "2025-02", "E2", "2", "20.0", "2.0", "g.csv", "2026-10-01T00:00:00", "Y", "BR",
                                "20.0", "2.0", "b", None, "h2")],
                              "customer_code string, reference_week string, product_ean string, unit_count string, "
                              "sales_value_brl string, sold_volume string, source_file string, load_date string, "
                              "competitor_flag string, market string, revenue string, qty_kg string, "
                              "_source_file string, _source_modified_at timestamp, _row_hash string")
    out = FatoBuilder(spark, engine, referencias, checkpoint=lambda d, n: d).harmonizar(
        {"fact_provider_a": a, "fact_provider_b": b})
    r = {x.provider: x for x in out.collect()}
    assert (r["A"].store_id, r["B"].store_id) == ("S1", "S2")      # customer_code -> store_id
    assert r["A"].competitor_flag is None and r["B"].average_price is None
    assert resultado(engine, "ING_003").falhas == 0
