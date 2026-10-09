"""Motor de DQ: check / correct / quarantine / com_status e o catálogo de regras."""
from pyspark.sql import functions as F

from conftest import resultado
from dq.engine import FLAGS, STATUS
from dq.models import Classificacao


def test_catalogo_rejeita_regra_inexistente(catalogo):
    import pytest
    with pytest.raises(KeyError):
        catalogo["XXX_999"]


def test_catalogo_tem_as_9_dimensoes(catalogo):
    dims = {r.dimensao.value for r in catalogo}
    assert dims == {"COMPLETUDE", "VALIDADE", "UNICIDADE", "CONSISTENCIA", "INTEGRIDADE_REFERENCIAL",
                    "ACURACIA", "TEMPORALIDADE", "COBERTURA", "RECONCILIACAO"}


def test_check_marca_sem_alterar_e_null_nao_conta_como_falha(spark, engine):
    df = spark.createDataFrame([("a", 1), ("b", -1), ("c", None)], "k string, v int")
    out = engine.check(df, "FCT_014", F.col("v") < 0, alvo="t", col_valor=None)
    linhas = {r.k: (r.v, r[FLAGS]) for r in out.collect()}
    assert linhas["b"] == (-1, ["FCT_014"])          # marcado, valor intacto
    assert linhas["c"] == (None, [])                 # comparação com NULL não vira falha
    assert resultado(engine, "FCT_014").falhas == 1


def test_correct_guarda_original_e_trilha(spark, engine):
    df = spark.createDataFrame([("a", " x "), ("b", "Y")], "k string, txt string")
    out = engine.correct(df, "PRD_004", F.col("txt") != F.upper(F.trim("txt")),
                         {"txt": F.upper(F.trim("txt"))}, alvo="t", chave=["k"], col_valor=None)
    r = {x.k: x for x in out.collect()}
    assert (r["a"].txt, r["a"].txt_raw) == ("X", " x ")
    assert r["b"].txt == "Y" and r["b"][FLAGS] == []
    trilha = engine._correcoes[-1].collect()
    assert len(trilha) == 1 and trilha[0].valor_antes == " x " and trilha[0].valor_depois == "X"


def test_quarantine_retira_e_guarda_payload(spark, engine):
    df = spark.createDataFrame([("a", 10.0), ("b", -5.0)], "k string, sales_value_brl double")
    out = engine.quarantine(df, "FCT_008", F.col("sales_value_brl") < 0, alvo="t", chave=["k"])
    assert [r.k for r in out.collect()] == ["a"]
    q = engine._quarentena[-1].collect()[0]
    assert q.regra_id == "FCT_008" and q.classificacao == "QUARENTENA" and '"k":"b"' in q.payload
    assert resultado(engine, "FCT_008").valor_impactado == 5.0     # R$ em valor absoluto


def test_status_respeita_a_precedencia(spark, engine):
    df = spark.createDataFrame([("a",)], "k string").withColumn(FLAGS, F.array(F.lit("PRD_004"), F.lit("PRD_003")))
    status = engine.com_status(df).first()[STATUS]
    # CORRIGIDO (PRD_004) < APROVADO_COM_ALERTA (PRD_003): vence o alerta
    assert status == Classificacao.APROVADO_COM_ALERTA.value


def test_flush_so_limpa_alvos_com_quarentena_ou_correcao(spark, engine):
    df = spark.createDataFrame([("a", 1)], "k string, v int")
    engine.record("GLD_001", "fato_vendas", avaliados=1, falhas=0)          # só log
    engine.quarantine(df, "COV_001", F.lit(False), alvo="cobertura", chave=["k"], col_valor=None)
    assert engine._alvos_com_saida == {"cobertura"}                       # 'fato_vendas' não é apagado
