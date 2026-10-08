"""Modelos do framework de Data Quality.

Uma regra é DECLARADA no YAML (config/dq_rules.yml) e EXECUTADA pelo motor (engine.py).
Aqui ficam só os tipos: classificação, severidade, dimensão e a especificação da regra.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import yaml


class Classificacao(str, Enum):
    """Resultado de uma regra para o registro. A ORDEM define a precedência: a maior vence."""

    APROVADO = "APROVADO"
    CORRIGIDO_AUTOMATICAMENTE = "CORRIGIDO_AUTOMATICAMENTE"
    APROVADO_COM_ALERTA = "APROVADO_COM_ALERTA"
    QUARENTENA = "QUARENTENA"
    REJEITADO = "REJEITADO"  # descartado com justificativa (ex.: cópia de duplicata exata)

    @property
    def rank(self) -> int:
        return list(Classificacao).index(self)


# Registros com estas classificações seguem para consumo (Gold)
CONSUMIVEIS = (
    Classificacao.APROVADO.value,
    Classificacao.CORRIGIDO_AUTOMATICAMENTE.value,
    Classificacao.APROVADO_COM_ALERTA.value,
)


class Severidade(str, Enum):
    CRITICA = "CRITICA"
    ALTA = "ALTA"
    MEDIA = "MEDIA"
    BAIXA = "BAIXA"
    INFO = "INFO"


class Dimensao(str, Enum):
    """As 9 dimensões de qualidade pedidas no enunciado (§5)."""

    COMPLETUDE = "COMPLETUDE"
    VALIDADE = "VALIDADE"
    UNICIDADE = "UNICIDADE"
    CONSISTENCIA = "CONSISTENCIA"
    INTEGRIDADE_REFERENCIAL = "INTEGRIDADE_REFERENCIAL"
    ACURACIA = "ACURACIA"
    TEMPORALIDADE = "TEMPORALIDADE"
    COBERTURA = "COBERTURA"
    RECONCILIACAO = "RECONCILIACAO"


@dataclass(frozen=True)
class Regra:
    """Especificação de uma regra, com os campos exigidos no enunciado.

    Resultado, volume avaliado e falhas não ficam aqui: são calculados a cada execução
    e gravados no log (ms_dq.rule_results).
    """

    id: str
    descricao: str
    dimensao: Dimensao
    escopo: str
    severidade: Severidade
    criterio: str
    tratamento: str
    se_falhar: Classificacao

    @classmethod
    def from_dict(cls, d: dict) -> "Regra":
        return cls(
            id=d["id"],
            descricao=d["descricao"],
            dimensao=Dimensao(d["dimensao"]),
            escopo=d["escopo"],
            severidade=Severidade(d["severidade"]),
            criterio=d["criterio"],
            tratamento=d["tratamento"],
            se_falhar=Classificacao(d["se_falhar"]),
        )


class CatalogoRegras:
    """Catálogo indexado por id. O código só pode usar regras documentadas no YAML."""

    def __init__(self, regras: list[Regra]):
        ids = [r.id for r in regras]
        duplicados = {i for i in ids if ids.count(i) > 1}
        if duplicados:
            raise ValueError(f"Regras duplicadas no catálogo: {sorted(duplicados)}")
        self._regras = {r.id: r for r in regras}

    @classmethod
    def from_yaml(cls, path: str | Path) -> "CatalogoRegras":
        with open(path, encoding="utf-8") as fh:
            itens = yaml.safe_load(fh)["regras"]
        return cls([Regra.from_dict(i) for i in itens])

    def __getitem__(self, regra_id: str) -> Regra:
        try:
            return self._regras[regra_id]
        except KeyError as exc:  # falha cedo: regra usada no código precisa estar no YAML
            raise KeyError(f"Regra '{regra_id}' não está em config/dq_rules.yml") from exc

    def __iter__(self):
        return iter(self._regras.values())

    def __len__(self) -> int:
        return len(self._regras)