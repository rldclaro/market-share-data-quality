"""Fixtures dos testes: SparkSession local e o catálogo/referências reais do projeto.

Os testes usam DataFrames pequenos montados à mão, um caso por regra (inclusive casos de borda),
e verificam o efeito no dado E o registro no log. Rodam fora do Databricks:
    pip install pyspark==4.0.1 pyyaml pytest
    pytest -q tests/
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
import yaml

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (SparkSession.builder.master("local[1]").appName("testes-dq")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.ansi.enabled", "true")      # igual ao serverless
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def catalogo():
    from dq.models import CatalogoRegras
    return CatalogoRegras.from_yaml(str(RAIZ / "config" / "dq_rules.yml"))


@pytest.fixture(scope="session")
def referencias():
    with open(RAIZ / "config" / "referencias.yml", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@pytest.fixture
def engine(spark, catalogo):
    from dq.engine import DQEngine
    return DQEngine(spark, catalogo, run_id="teste")


def resultado(engine, regra_id):
    """Último resultado registrado da regra."""
    return [r for r in engine._resultados if r.regra.id == regra_id][-1]
