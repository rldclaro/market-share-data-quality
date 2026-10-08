# Market Share — Data Quality

Pipeline em PySpark que consolida dados semanais de mercado de dois fornecedores e
gera uma base confiável para cálculo de Market Share.

O foco é qualidade de dados: identificar problemas nas dimensões e nas fatos, corrigir
automaticamente apenas o que é seguro, mandar para quarentena o que exige análise e
manter rastreabilidade do dado original.

## Stack

- Databricks (Free Edition, serverless)
- PySpark + Delta Lake
- Unity Catalog (schemas, tabelas e volumes)
- Databricks CLI / Asset Bundles

## Arquitetura (Unity Catalog)

| Objeto | Tipo | Conteúdo |
|---|---|---|
| `workspace.ms_raw.landing` | Volume | arquivos originais do case (imutáveis) |
| `workspace.ms_bronze` | Schema | dados brutos em Delta, sem tratamento, com metadados de rastreio |
| `workspace.ms_silver` | Schema | dimensões e fato tratadas |
| `workspace.ms_gold` | Schema | Market Share pronto para consumo |
| `workspace.ms_dq` | Schema | log de regras, quarentena e métricas de qualidade |

## Estrutura do repositório

| Pasta | Conteúdo |
|---|---|
| `data/raw/` | dados de entrada do case (não alterados) |
| `docs/` | documentação e matriz de requisitos |
| `scripts/` | automação do ambiente (Databricks CLI) |
| `notebooks/` | notebooks Databricks (formato `.py`, versionável) |
| `databricks.yml` | definição do projeto como código (Databricks Asset Bundle) |

## Como executar

### 1. Pré-requisitos

- Conta no Databricks Free Edition
- Databricks CLI instalada (`databricks -v`)
- Login no workspace:

```bash
databricks auth login --host <URL_DO_WORKSPACE> --profile gf7
```

### 2. Preparar o Unity Catalog e enviar os dados

```bash
./scripts/setup_uc.sh            # perfil e catálogo opcionais: ./scripts/setup_uc.sh gf7 workspace
```

Cria os schemas `ms_raw`, `ms_bronze`, `ms_silver`, `ms_gold` e `ms_dq` no catálogo
`workspace` e envia os arquivos de `data/raw` para o volume `ms_raw.landing`.
O script é idempotente: pode ser executado várias vezes sem efeito colateral.

### 3. Publicar o projeto no workspace (Asset Bundle)

```bash
databricks bundle validate -t dev --profile gf7
databricks bundle deploy   -t dev --profile gf7
```

O deploy envia os notebooks para `/Workspace/Users/<usuario>/.bundle/market_share_dq/dev/files`
e cria os jobs definidos em `databricks.yml`. Os dados não são enviados pelo bundle
(`sync.exclude`): a única fonte é o volume `ms_raw.landing`.

### 4. Executar

```bash
databricks bundle run -t dev hello_job --profile gf7
```

Todos os jobs rodam em computação serverless (único tipo disponível no Free Edition).
Para desenvolvimento interativo, abra o notebook na pasta do bundle e selecione **Serverless**.

## Dados sensíveis

O arquivo `sensitive_store_contacts.csv` contém dados pessoais de contato e não é necessário
para o cálculo de Market Share. Por minimização (LGPD), ele não é versionado no Git
(`.gitignore`) e não é enviado ao Databricks.

## Status

Em desenvolvimento. Acompanhamento dos requisitos em
[docs/00_rastreabilidade.md](docs/00_rastreabilidade.md).