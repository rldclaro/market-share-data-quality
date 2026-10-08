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
| `src/dq/` | framework de Data Quality (classes reutilizáveis) |
| `config/` | catálogo de regras de DQ (`dq_rules.yml`) |
| `sql/profiling/` | consultas que evidenciam cada problema encontrado |
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

### 4. Executar o pipeline

```bash
databricks bundle run -t dev pipeline_job --profile gf7
```

Todos os jobs rodam em computação serverless (único tipo disponível no Free Edition).
Para desenvolvimento interativo, abra o notebook na pasta do bundle e selecione **Serverless**.

## Camadas

### Bronze (`notebooks/01_bronze.py`)

Cópia fiel dos arquivos de origem em tabelas Delta (`ms_bronze.*`), sem nenhuma correção.

- **Todas as colunas como string.** A inferência de tipos pode transformar um valor fora do
  padrão em nulo ou mudar o tipo da coluna sem aviso. Lendo como texto, nada se perde; a
  conversão acontece de forma explícita na Silver, e o que falhar vai para quarentena com o
  valor original.
- **Rastreabilidade por linha:** arquivo de origem (`_source_file`), data do arquivo
  (`_source_modified_at`), hash do conteúdo (`_row_hash`) e execução que carregou a linha
  (`_load_id`).
- **Reconciliação:** linhas carregadas × volumes declarados no README do case, e checksum
  SHA-256 de cada arquivo, registrados em `ms_dq.bronze_load_log`. Divergência interrompe o
  pipeline.
- **Idempotência:** as tabelas são sobrescritas a cada carga (reexecutar não duplica); o log é
  append-only (histórico de auditoria).

| Tabela | Linhas |
|---|---|
| `dim_produto` | 1.235 |
| `dim_loja` | 2.500 |
| `dim_calendario` | 91 |
| `customer_territory_history` | 4.736 |
| `coverage_provider` | 6.370 |
| `fact_provider_a` | 68.526 |
| `fact_provider_b` | 58.526 |

## Framework de Data Quality (`src/dq/`)

Toda regra é declarada em `config/dq_rules.yml` com os campos exigidos pelo case:
**id, descrição, dimensão, escopo, severidade, critério, tratamento** e a classificação aplicada
quando ela falha. Resultado, volume avaliado, falhas e valor impactado são calculados a cada
execução e gravados no log. O código só pode usar regras que estão no catálogo.

| Operação | Efeito | Rastro |
|---|---|---|
| `check` | marca a falha no registro (`dq_flags`); o dado não muda | log |
| `correct` | aplica correção determinística; o original fica em `<coluna>_raw` | log + `ms_dq.corrections` (antes → depois) |
| `quarantine` | retira o registro do fluxo | log + `ms_dq.quarantine` (regra, severidade, motivo, arquivo de origem, payload original) |
| `record` | regra de nível de dataset (ex.: reconciliação) | log |

Cada registro recebe `dq_status`, a classificação de maior precedência entre as regras que violou:

`APROVADO` < `CORRIGIDO_AUTOMATICAMENTE` < `APROVADO_COM_ALERTA` < `QUARENTENA` < `REJEITADO`

`REJEITADO` é usado para o que é descartado com justificativa (ex.: cópia de duplicata exata);
`QUARENTENA` é o que aguarda análise.

| Tabela | Escrita | Por quê |
|---|---|---|
| `ms_dq.rule_results` | append | histórico de execuções, base para tendência no monitoramento |
| `ms_dq.quarantine` | substitui por alvo (`replaceWhere`) | reexecutar uma etapa não duplica a quarentena dela nem apaga a de outra etapa |
| `ms_dq.corrections` | substitui por alvo (`replaceWhere`) | idem |

O motor é compatível com computação serverless: não usa `cache()`, `sparkContext` nem RDD, e
calcula avaliados, falhas e valor impactado em uma única passada por regra.

## Dados sensíveis

O arquivo `sensitive_store_contacts.csv` contém dados pessoais de contato e não é necessário
para o cálculo de Market Share. Por minimização (LGPD), ele não é versionado no Git
(`.gitignore`) e não é enviado ao Databricks.

## Status

Em desenvolvimento. Acompanhamento dos requisitos em
[docs/00_rastreabilidade.md](docs/00_rastreabilidade.md).