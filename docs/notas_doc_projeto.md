# Notas para o documento do projeto (acumuladas por etapa)

## Conceitos explicados

### Databricks Asset Bundle
- Projeto do Databricks descrito em arquivos (`databricks.yml`) e publicado com um comando: infraestrutura como código.
- Sem bundle: job criado clicando na UI, sem histórico e difícil de reproduzir. Com bundle: a "receita" fica escrita; qualquer workspace obtém o mesmo resultado com `databricks bundle deploy`.
- Partes do nosso `databricks.yml`: `bundle.name` (projeto), `sync.exclude` (o que não vai para o workspace — os dados), `variables.catalog` (parâmetro), `resources.jobs` (jobs criados/atualizados), `targets.dev` (ambiente; depois hml/prd).
- Responde à Q7 ("como levar para produção"): versionado no Git, reproduzível, ambientes separados, automatizável (GitHub Actions + `bundle deploy -t prd` = CI/CD, N08).
- Frase de entrevista: "Usei Databricks Asset Bundles para definir notebooks e jobs como código, garantindo deploy reproduzível, versionado e separado por ambiente — base para CI/CD."
- `mode: development` prefixa recursos com [dev usuario] e não agenda nada.
- Host do workspace fica fora do YAML (vem do perfil da CLI) → repo não fica amarrado a um workspace.

### Unity Catalog (hierarquia catálogo.schema.objeto)
- `workspace` (catálogo) → `ms_raw` (volume `landing`), `ms_bronze`, `ms_silver`, `ms_gold`, `ms_dq`.
- Tabela = dado estruturado (Delta). Volume = arquivos (CSV, JSON...) — substituto moderno do DBFS.
- Um schema por camada: Medallion visível no catálogo + permissões por camada. Prefixo `ms_` evita colisão.
- Volume MANAGED: Databricks gerencia o storage (único tipo no Free Edition).
- `dbfs:/Volumes/...` na CLI é só endereçamento; o arquivo vai para o Unity Catalog.

### Databricks Free Edition / serverless — restrições que afetam o código
- Só computação serverless; Spark 4.2.0 observado no workspace.
- `df.cache()/persist()/checkpoint()` lançam exceção; RDD/sparkContext não existem (Spark Connect).
- DBFS limitado → usar Volumes; caminhos e imports relativos podem falhar → caminhos absolutos.
- ANSI mode ligado (divisão por zero falha → `try_divide`).
- Internet de saída restrita a domínios confiáveis.

### Autenticação
- CLI: `databricks auth login` (OAuth U2M). Perfil `gf7` em `~/.databrickscfg`, fora do repo e no `.gitignore`. Token temporário e renovável; nenhum segredo no código.
- GitHub pelo VS Code (HTTPS com credencial gerenciada pelo editor).

### Idempotência (primeira aparição: scripts/setup_uc.sh)
- `if get ... else create`: rodar 2x dá o mesmo resultado, sem erro de "já existe". Mesmo princípio aplicado no pipeline (N10).
- `set -euo pipefail`: para no primeiro erro.
- Perfil e catálogo parametrizados: funciona em outro workspace sem editar código.

### LGPD / minimização (D11, N03)
- `sensitive_store_contacts.csv` (nome, e-mail, telefone) não é necessário para Market Share.
- Não versionado (`.gitignore`) e não enviado ao Databricks (filtro no `setup_uc.sh`). Princípio: dado pessoal que o processo não usa não deve nem entrar na plataforma.

### Bronze lê tudo como string (pendente: frase do Rildo)
1. Nada se perde em silêncio: inferência transforma valor fora do padrão em NULL ou muda o tipo da coluna.
2. O erro vira dado: conversão explícita na Silver (`try_cast`); falha vai para quarentena com o texto original.
3. Schema estável: inferência pode variar entre arquivos.
- Evidência no notebook 00_hello: ícone "A͟c" (string) em todas as colunas, inclusive year/month/week.

### Repositório
- Projeto dentro do Linux (`~/projetos`), não em `/mnt/c` (lento, permissões erradas).
- CSVs do case versionados (sintéticos, fornecidos para avaliação, repo privado) → reprodutível; exceto o sensível.
- `data/raw/README.md` = README original dos dados, imutável.
- Commits: Conventional Commits curtos (`tipo: descrição`, imperativo).
- `chore` = infraestrutura/configuração; `feat` = funcionalidade do pipeline.

### Formato de notebook `.py` (# MAGIC)
- Arquivo é Python válido; células não-Python ficam como comentários: `# Databricks notebook source` (1ª linha), `# COMMAND ----------` (divisória), `# MAGIC %md` / `%sql` (células markdown/SQL).
- Nome vem dos "magic commands" (%md, %sql, %pip, %run) do Jupyter.
- Vantagens sobre .ipynb: diff legível, saídas (dados) não vão para o arquivo, revisável em PR.

### Notebook × task × job (pipeline)
- Notebook = código de uma etapa; Task = "rode este notebook com estes parâmetros"; Job = ordem das tasks (`depends_on`), agenda, alertas.
- `pipeline_job` no databricks.yml: bronze → silver_dimensoes → silver_fato → gold. Se uma task falha (ex.: assert da reconciliação), as seguintes não rodam: dado ruim não se propaga.
- Não confundir com Lakeflow Declarative Pipelines (antigo DLT): não usado, pois Jobs+notebooks dão controle total sobre regras de DQ, quarentena e log.

### Gold: tabela materializada + views de consumo
- Só view não: fura os portões de publicação (mostra Silver mesmo com validação falha), perde auditoria (sem time travel do que foi publicado), recalcula a cada consulta.
- Padrão: `ms_gold.market_share` (tabela Delta validada, fonte da verdade) + `vw_market_share_oficial` / `vw_share_nestle` (consumo).
- Views: contrato estável para o BI, filtro "somente OFICIAL", menor privilégio (acesso só à view), comentários de coluna no UC.
- Materialized Views do Databricks: citadas como evolução (exigem pipeline Lakeflow, limitado no Free Edition, sem portão de validação).
- Frase: "Gold materializada em Delta para validar antes de publicar, time travel e custo previsível; consumo por views com contrato estável, regra de oficial e acesso mínimo."

### Bronze (Etapa 3) — decisões
- Tudo string (`inferSchema=False`); nomes de coluna limpos (BOM defensivo).
- `_metadata.file_name` / `file_modification_time` capturados antes de qualquer projeção (coluna oculta).
- `_row_hash` = sha256 do conteúdo da linha → identifica duplicatas exatas.
- `_load_id` = `{{job.run_id}}` → cada linha ligada à execução.
- Tabelas em `overwrite` (idempotente); log `ms_dq.bronze_load_log` em `append` (histórico de auditoria). Dado se substitui; auditoria se acumula.
- sha256 de cada arquivo no log = prova de imutabilidade da entrada (D01).
- Reconciliação com volumes do README; `assert` falha a task se divergir → bloqueia o pipeline.
- Resultado: 7 tabelas, todos os volumes batem com o README.

## Achados do diagnóstico (vira Q1 / E25)
1. Arquivos CSV com BOM UTF-8 (`EF BB BF`) no cabeçalho. O leitor do Spark remove; outras ferramentas (pandas sem utf-8-sig, scripts simples) não — primeira coluna viraria `﻿date`. Tratamento: limpeza defensiva dos nomes de coluna na Bronze. Evidência: `f.read(3) == b'\xef\xbb\xbf'` e `df.columns[0] == 'date'`.
3. **Duplicatas exatas:** 250 linhas excedentes em A e 250 em B (mesmo `_row_hash`). Efeito: venda contada 2x, infla marca e denominador de forma desigual → MS distorcido sem sinal visível. Tratamento: remover cópias (seguro: conteúdo idêntico), registrando como REJEITADO com payload.
4. **Chaves conflitantes (mesma semana×loja×EAN, valores diferentes):** 11 chaves em A (22 linhas). Hipótese levantada: "movimentações" a somar. Rejeitada porque: (a) o grão é agregado semanal — 1 linha por chave por definição; (b) a maioria das versões vem do MESMO arquivo (ex.: provider_a_2025-05.csv carregado em 01/10 e 04/10), 6 un vs 118 un; (c) preço médio incoerente entre versões (R$ 37,04 vs R$ 6,56; R$ 30,43 vs R$ 18,12) — o produto não muda tanto de preço na mesma semana/loja; (d) 2 casos têm uma versão no arquivo `provider_a_series_*` (carga 05/10 09:00) → segunda fonte para o mesmo fato. Decisão (Rildo): QUARENTENA de todas as versões. Alternativas descartadas: somar (contagem dupla se for correção), mais recente (timestamp não prova correção, sem nº de versão), maior valor (arbitrário). Saída para o negócio: se o fornecedor confirmar que a carga mais recente substitui, a regra vira "última versão vence" por configuração. B: pendente.
2. Calendário semanal (segunda a domingo); 91 semanas = volume do README → granularidade temporal do MS.

## Decisões técnicas registradas
| Decisão | Motivo |
|---|---|
| Databricks Free Edition + serverless | ambiente pedido; sem Spark/Java local |
| Um schema por camada (ms_raw/bronze/silver/gold/dq) | Medallion explícita, permissões por camada |
| Dados só no Volume (`sync.exclude: data/**`) | uma única fonte; evita cópia como workspace file |
| Notebooks em `.py` (formato fonte Databricks) | diff legível no Git |
| Bundle sem host no YAML | portabilidade entre workspaces |
| Nome do repo sem "GF7" | não expor código interno; portfólio |

## Etapa 5 — dimensão de produto
- Resultado validado no Databricks: 1.200 linhas / 1.200 EANs; 1.098 aprovados, 71 corrigidos, 31 alerta.
- Sequência de regras e por que a ordem importa: texto → sinônimos → hierarquia (calculada DEPOIS da grafia, senão LACTEO/LACTEOS dividem a evidência) → vazio → dedup EAN → conflito c/ evidência → conflito s/ evidência → EAN malformado.
- Armadilha do de-para: corrige a ESCRITA, não o SIGNIFICADO (TEMPEROS + CHOCOLATE → CHOCOLATES continua errado) → precisa da hierarquia.
- Caso exemplo P00334 MAGGI TEMPEROS 100G: CHOCOLATE → CHOCOLATES (PRD_005) → CULINARIOS (PRD_007); dq_flags [PRD_005, PRD_007], 2 linhas em corrections.
- Hierarquia derivada dos dados (ideia do Rildo: relação 1 subcategoria : 1 categoria), trava de 90%; evidência 96,7–98,5%; tabela ms_silver.ref_hierarquia_produto.
- Erro tipo "d" (categoria válida mas errada) não é pego por validação de domínio, só pela hierarquia.
- EAN malformado NÃO corrigido: existe igual no cadastro e na fato (EAN-00000635 → P00635); corrigir um lado quebra o join. ean_sugerido = 7891000 + nº do product_id.
- Catálogo real Nestlé Professional (PDF) avaliado e NÃO usado: dados do case são sintéticos (join ~0), README proíbe dados reais no repo, taxonomia diferente. Uso como argumento: EANs reais têm 13 dígitos e prefixo 7891000 (GS1 Brasil = 789) → reforça que 891... perdeu o 7. Produção: MDM + dígito verificador GS1 + Cadastro Nacional de Produtos.
- Notebooks de validação movidos para notebooks/dev (hello, demo_dq_engine); demo limpa o que grava; raiz do projeto encontrada subindo até o databricks.yml.