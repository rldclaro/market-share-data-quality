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
| `src/silver/` | regras de tratamento de cada entidade da Silver |
| `config/` | catálogo de regras de DQ (`dq_rules.yml`) e referências das correções (`referencias.yml`) |
| `sql/profiling/` | consultas que evidenciam cada problema encontrado (antes do tratamento) |
| `sql/validacao/` | consultas que provam o tratamento: trilha, reconciliação log × trilha e garantias |
| `notebooks/` | notebooks do pipeline (formato `.py`, versionável) |
| `notebooks/dev/` | notebooks de validação do ambiente e do framework (fora do pipeline) |
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

### Silver · dimensão de produto (`src/silver/produto.py`)

Resultado: **1.235 registros → 1.200 produtos, 1 por EAN** (1.098 aprovados, 71 corrigidos,
31 com alerta).

| Regra | Problema | Tratamento | Registros |
|---|---|---|---|
| PRD_004 | texto despadronizado (`delta tabletes 1l  `) | trim + maiúsculas | 95 corrigidos |
| PRD_005 | grafia da categoria (`CHOCOLATE`, `LACTEO`) | de-para para o domínio oficial | 6 corrigidos |
| PRD_006 | categoria vazia | deriva da hierarquia subcategoria → categoria | 3 corrigidos |
| PRD_002 | EAN duplicado (registro `PX####` "NOVA EMB" com categoria `OUTROS`) | mantém o id canônico `P#####`; o alias é rejeitado e listado em `alias_product_ids` | 35 rejeitados |
| PRD_007 | categoria válida, porém em conflito com a subcategoria (ex.: `CEREAIS` em `BEBIDAS`) | corrige pela hierarquia **somente** se a descrição confirma a subcategoria | 8 corrigidos |
| PRD_008 | conflito sem evidência na descrição | mantém + alerta | 0 |
| PRD_003 | EAN fora do padrão de 13 dígitos (`891…`, `EAN-000…`) | **não corrige**: a fato usa o mesmo código e corrigir só o cadastro quebraria o join. Gera `ean_sugerido` para o time de cadastro | 31 alertas |

**Por que deduplicar o EAN:** ele é a chave do join com a fato. Com dois registros para o mesmo
EAN, cada venda desses produtos seria contada duas vezes.

**Hierarquia derivada dos dados.** Não existe cadastro mestre de categorias no case, então o
de-para subcategoria → categoria é calculado a partir dos próprios produtos (categoria dominante)
e gravado com a evidência em `ms_silver.ref_hierarquia_produto`. Só é usado para corrigir quando a
concordância é de pelo menos 90% (configurável); as 5 subcategorias ficaram entre 96,7% e 98,5%.

| Subcategoria | Categoria oficial | Evidência |
|---|---|---|
| CAFE | BEBIDAS | 98,2% |
| CEREAIS | NUTRICAO | 96,7% |
| LEITE EM PO | LACTEOS | 98,5% |
| TABLETES | CHOCOLATES | 96,8% |
| TEMPEROS | CULINARIOS | 98,0% |

**Premissa:** a subcategoria é o atributo confiável (sem nulos, sem variação de grafia e presente
na descrição do produto); a categoria é derivada dela. Em produção, essa referência viria do
cadastro mestre (MDM) e o EAN seria validado pelo dígito verificador GS1.

### Silver · dimensão de loja (`src/silver/loja.py`)

Resultado: **2.500 lojas** (2.285 aprovadas, 48 corrigidas, 167 com alerta).

| Regra | Problema | Tratamento | Registros |
|---|---|---|---|
| LOJ_001 | `store_id` duplicado | quarentena | 0 |
| LOJ_002 | UF incoerente com a cidade: nome de cidade no campo UF (`São Paulo` em lojas de BH, Curitiba…), `XX` e vazio | corrige pela UF dominante da cidade | 20 corrigidos |
| LOJ_003 | CNPJ com máscara (`10.000.000/0000-28`) | remove a máscara quando restam 14 dígitos | 35 corrigidos |
| LOJ_004 | coordenada fora do território brasileiro | **não corrige**: alerta + fila de revalidação | 10 alertas |
| LOJ_005 | coordenada sem relação com a cidade (regra de dataset) | alerta: campo fora do Market Share | 2.490 |
| LOJ_006 / LOJ_007 | território / vendedor vazios | alerta (o território é resolvido na fato) | 90 / 70 |

**UF derivada dos dados.** Mesmo critério da hierarquia de produto: a UF oficial de cada cidade é
a dominante entre as lojas com UF válida, gravada com a evidência em `ms_silver.ref_cidade_uf`
(11 cidades, todas com 100% de concordância). Um de-para `São Paulo → SP` erraria as lojas de BH e
Curitiba: a cidade é o atributo confiável.

**Por que a coordenada não foi corrigida (LOJ_004).**

1. O critério usa os pontos extremos oficiais do território brasileiro (IBGE, *Brasil em Síntese*),
   versionados em `config/referencias.yml`: N 5,271944 (Monte Caburaí) · S -33,751944 (Arroio Chuí) ·
   L -34,792778 (Ponta do Seixas) · O -73,990556 (nascente do rio Moa).
2. As 10 lojas fora desses limites têm forte indício de **latitude e longitude invertidas**: trocando
   os campos, todas voltam à faixa das demais lojas.
3. Mas o campo inteiro **não corresponde à cidade** (LOJ_005): o centro médio das coordenadas varia só
   1,15° entre cidades, contra uma dispersão de 4,38° dentro de cada cidade; a correlação lat × lon é
   -0,03; apenas 0,3% das lojas estão a menos de 50 km da própria cidade. Invertidas, as 10 lojas
   ficariam de 227 a 1.407 km da cidade informada.
4. Sem referência confiável, corrigir trocaria um erro visível (ponto no oceano) por um erro invisível
   (ponto no Brasil, no lugar errado). A coordenada não entra no cálculo de Market Share, então a
   correção traria risco sem ganho.

As lojas ficam com `geo_status = REVALIDAR_INVERSAO` e entram na fila `ms_dq.vw_revalidacao_loja`
junto com as demais pendências de cadastro (UF corrigida, território e vendedor vazios), cada uma com
problema, evidência e ação sugerida. O retângulo dos pontos extremos é uma checagem necessária, não
suficiente (inclui oceano e países vizinhos); a validação real depende da melhoria abaixo.

### Silver · histórico de território (`src/silver/territorio.py`)

Histórico SCD2 por **loja × categoria** (o território depende da categoria): 4.736 vigências.

| Regra | Problema | Tratamento | Registros |
|---|---|---|---|
| TER_004 | loja do histórico inexistente na `dim_loja` | quarentena | 0 |
| TER_006 | duas vigências começando no mesmo dia | quarentena | 0 |
| TER_001 | vigências sobrepostas: CRM aberta desde 2025 × realocação MANUAL a partir de 2026-01-01 | fecha a vigência anterior em (próximo início − 1 dia) | 121 corrigidos |
| TER_002 / TER_005 | território / vendedor vazios | alerta | 169 / 130 |
| TER_003 | loja sem nenhuma vigência (regra de dataset) | alerta | 11 lojas |

**Por que fechar a vigência (TER_001).** Em 2026, cada uma das 121 combinações tinha dois territórios
vigentes: o join com a fato devolveria duas linhas e duplicaria a venda no recorte por território. A
vigência que começa depois vence: em 121 de 121 casos o território é diferente (realocação, não erro
de digitação) e em 5 deles o MANUAL preenche um CRM vazio. O histórico anterior é preservado: as
vendas de 2025 continuam no território antigo.

| store_id | category | territory_id | valid_from | valid_to (origem → Silver) | origem |
|---|---|---|---|---|---|
| S00009 | NUTRICAO | T010 | 2025-01-01 | 9999-12-31 → **2025-12-31** | CRM |
| S00009 | NUTRICAO | T022 | 2026-01-01 | 9999-12-31 | MANUAL |

**Território usado na fato (resolvido na Etapa 6):** vigência do histórico na semana (loja ×
categoria) → `dim_loja.territory_id` → `SEM_TERRITORIO`. A `dim_loja` reproduz o território do CRM
em 100% dos casos, por isso serve de fallback para as 11 lojas sem histórico e para as categorias sem
vigência própria.

**Premissa a validar com o negócio:** a atribuição MANUAL é uma realocação intencional do time comercial.

### Silver · calendário (`src/silver/calendario.py`)

Grão semanal **ISO 8601** (segunda a domingo): 91 semanas, de `2025-02` a `2026-40`.

| Regra | Problema | Tratamento | Registros |
|---|---|---|---|
| CAL_003 | `year_week` duplicado ou incoerente com o início da semana | rejeita (bloqueia o pipeline) | 0 |
| CAL_002 | semana incompleta ou com lacuna | rejeita (bloqueia o pipeline) | 0 |
| CAL_001 | ano civil da segunda-feira ≠ ano ISO | corrige para o ano da quinta-feira | 1 corrigido |
| CAL_004 | mês civil da segunda-feira ≠ mês ISO | corrige para o mês da quinta-feira | 9 corrigidos |

**Por que padronizar em ISO.** A origem misturava duas convenções: `year_week` e `week` em ISO, e
`year`/`month` pela data civil da segunda-feira. Elas divergem nas semanas que cruzam a virada do mês
— a semana `2026-01` (29/12/2025 a 04/01/2026) vinha com `year = 2025` e `month = 12`. No ISO, a
semana pertence ao ano e ao mês da **quinta-feira**, o dia do meio: é o mês que contém 4 dos 7 dias
(mês real da semana). Os valores originais ficam em `year_raw` e `month_raw`.

**Efeito:** a contagem de semanas por mês muda em alguns meses (ex.: dezembro/2025 passa de 5 para 4
semanas e janeiro/2026 de 4 para 5), e a semana aberta `2026-40` passa a pertencer a outubro/2026.
O Market Share semanal não muda; agregações mensais passam a seguir o mês real.

### Validação da Silver (`sql/validacao/01_silver_dimensoes.sql`)

| Bloco | O que prova |
|---|---|
| Log | resultado de cada regra na última execução e o histórico entre execuções |
| Trilha | antes → depois de cada correção, por regra e por registro |
| Reconciliação | registros na trilha = falhas no log, e quarentena = falhas no log (diferença 0) |
| Antes × depois | `<coluna>_raw` × valor tratado na própria tabela |
| Garantias | 1 linha por EAN e por loja, UF no domínio, CNPJ com 14 dígitos, coordenada **não alterada** (Silver = Bronze), vigências sem sobreposição, calendário em ISO e íntegro — todas com 0 violações |

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

## Próximas melhorias

- **Geolocalização de lojas.** Incluir endereço e CEP na `dim_loja` a partir do cadastro/CRM,
  geocodificar o CEP por base oficial e criar a regra "distância entre a coordenada informada e a do
  CEP > 50 km → alerta". A validação de fronteira passaria do retângulo dos pontos extremos para o
  polígono oficial do IBGE. Endereço de loja (pessoa jurídica) não é dado pessoal, mas deve ficar
  separado dos contatos sensíveis.
- **Cadastro mestre.** Hierarquia de produto e UF por cidade vindas de MDM, em vez de derivadas dos dados.

## Dados sensíveis

O arquivo `sensitive_store_contacts.csv` contém dados pessoais de contato e não é necessário
para o cálculo de Market Share. Por minimização (LGPD), ele não é versionado no Git
(`.gitignore`) e não é enviado ao Databricks.

## Status

Em desenvolvimento. Acompanhamento dos requisitos em
[docs/00_rastreabilidade.md](docs/00_rastreabilidade.md).