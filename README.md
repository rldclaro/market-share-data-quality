# Market Share — Data Quality

Pipeline em PySpark que consolida dados semanais de mercado de dois fornecedores e
gera uma base confiável para cálculo de Market Share.

O foco é qualidade de dados: identificar problemas nas dimensões e nas fatos, corrigir
automaticamente apenas o que é seguro, mandar para quarentena o que exige análise e
manter rastreabilidade do dado original.

## Stack

- Databricks (Free Edition, serverless)
- PySpark + Delta Lake
- Unity Catalog (tabelas e volumes)
- Databricks CLI / Asset Bundles

## Estrutura

| Pasta | Conteúdo |
|---|---|
| `data/raw/` | dados de entrada do case (não alterados) |
| `docs/` | documentação e matriz de requisitos |

## Status

Em desenvolvimento. Acompanhamento dos requisitos em
[docs/00_rastreabilidade.md](docs/00_rastreabilidade.md).