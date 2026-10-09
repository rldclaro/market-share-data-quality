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

## Etapa 5 — loja, território e calendário
- Resultado validado no Databricks (run 255988177559075): dim_loja 2.500 (2.285 aprov., 48 corr., 167 alerta); território 4.736 vigências, 0 sobreposições; log com 24 regras.
- **UF (LOJ_002):** "São Paulo" é nome de CIDADE no campo de ESTADO → de-para erraria BH/Curitiba. UF derivada da cidade (dominante, trava 90%), mesma técnica da hierarquia de produto. 20 lojas.
- **CNPJ (LOJ_003):** maioria sem máscara → padrão; só formatação, 14 dígitos, sem duplicar. 35 lojas.
- **Coordenada (LOJ_004/005) — decisão do Rildo: não alterar, alertar.** Critério com fonte oficial (pontos extremos IBGE). As 10 têm indício de inversão, mas o campo inteiro é sintético (centro das cidades igual, correlação ~0, 0,3% a < 50 km). Corrigir = trocar erro visível por erro invisível. Coordenada fora do MS. Fila `ms_dq.vw_revalidacao_loja` + melhoria endereço/CEP. Erro meu admitido: eu tinha afirmado que inverter "acertava" a localização; a verificação de distância mostrou que não.
- **Vigências (TER_001):** 121 sobreposições CRM 2025 × MANUAL 2026 → venda duplicaria no join. Fecha em próximo início − 1 dia (SCD2). Território depende de loja × categoria. Fallback: histórico → dim_loja → SEM_TERRITORIO.
- **Calendário (CAL_001/004) — decisão do Rildo: ISO.** Fonte misturava ISO (year_week) e civil (year/month da segunda). Quinta-feira = dia do meio = mês com 4 de 7 dias ("mês real"). 1 ano + 9 meses corrigidos. Efeito: semanas por mês mudam; 2026-40 vai para outubro.
- Princípio consolidado: corrige o que tem prova (UF, CNPJ, vigência, calendário); alerta o que não tem (coordenada, EAN malformado); toda correção com `_raw` + `corrections` + reconciliação log × trilha (`sql/validacao`).

## Etapa 6 — fato A + B
- Harmonização por configuração + `unionByName(allowMissingColumns=True)` (casa por nome). ING_002 interrompe, ING_003 alerta drift.
- Ordem: retira → corrige → alerta. Conservação 127.052 = 124.861 + 2.191 (assert no notebook).
- Decisões padrão confirmadas pelo Rildo: A×B quarentena; negativo corrige só com prova (A, units×preço); preço fora da faixa e pico curto → quarentena; evento sustentado → alerta; volume em kg (secundário); território pela quinta-feira.
- Rildo pediu broadcast em todo join com tabela menor e unionByName: aplicado nas dimensões; agregados por série (~119 mil, quase o tamanho da fato) ficam sem broadcast de propósito (escala; AQE decide).
- Erro do Databricks Assistant: sugeriu limiares inventados (fator 3, min 5 semanas) — rejeitados, mudariam o resultado sem base.
- Fila de tratativa `vw_quarentena_fato` (ação + responsável no YAML); `vw_revalidacao_loja` com valor_origem × valor_silver (pedido do Rildo para ficar claro que a Silver já corrigiu).

## Etapa 7 — Gold
- Cobertura conservadora (não recebido = 0; recebidas ≤ esperadas). Separa ausência de venda × ausência de cobertura.
- MS por valor (principal) e volume; 5 recortes; mesma base no numerador e denominador.
- Portões: staging → validação → publicação. GLD_001/002/003/005 bloqueiam; GLD_004 marca não oficial com motivo.
- Bug evitado: flush apagaria a quarentena da fato (GLD_001 com alvo fato_vendas) → motor passou a limpar só alvos com quarantine/correct.
- Tabela + views (oficial, Nestlé, cobertura). LGPD: contatos nunca ingeridos + GLD_005.

## Etapa 8/9 — validação e fechamento
- Pente fino: 41 verificações, 41/41 OK no Databricks.
- pytest: 13 testes; acharam 2 casos de borda (corr com coordenada constante; MAD = 0), corrigidos sem mudar resultados.
- Evidências exportadas + 4 gráficos; alerta por e-mail no job (usuário da CLI, sem e-mail no repo).
- Matriz: obrigatórios 36/36; nice to have 11/13 (CI/CD e incremental documentados como próximos passos).
- Pedido do Rildo: gerar as evidências dentro do projeto. Virou a 5ª task do job (`05_evidencias.py` + `src/relatorios/evidencias.py`): roda o pente fino como portão final (FALHA derruba o job), exporta CSVs e gráficos para o Volume `ms_dq.evidencias`; `scripts/baixar_evidencias.sh` traz para `docs/evidencias/`.
- Gold, cobertura e fixes ainda não estavam commitados no repo (visto no `git status` de 08/10 23h19) — commits separados por assunto.

## Etapa 10 — CI/CD e revisão das evidências
- GitHub Actions (`ci_cd.yml`): pytest em todo push/PR; `bundle validate` + `deploy` na `main` só com CI verde. Runs #1/#2 falharam (requirements-dev ausente; `src/` desatualizado — o CI barrou deploy de código não testado); #3–#5 verdes. Matriz: nice to have 12/13.
- Bug no gráfico 02 (visto pelo Rildo na evidência gerada): título saía "R$ 3.1 mi" — um `.replace(",", ".")` aplicado ao título inteiro desfazia a vírgula decimal do `_brl`. Correção: `_milhar()` formata cada número isoladamente → "R$ 3,1 mi em 2.191 linhas". Só apresentação; números inalterados.
- FCT_005 e FCT_008 com 566 linhas cada: conjuntos distintos (retirada é sequencial — linha removida por FCT_005 não chega à FCT_008; valores R$ 620.614 × R$ 661.110). Coincidência da geração sintética.
- Revisão do gráfico 01 (share Nestlé nacional): a série semanal oscila ~6 p.p. por semana sem persistência (autocorrelação ≈ 0 em todas as categorias) — ruído, não tendência. Passou a mostrar a média móvel de 4 semanas **oficiais** (semanal ao fundo, ○ nas não oficiais). Status da semana agora é explícito (não oficial se qualquer marca for não oficial), em vez de `max(ms_status)`, que dependia da ordem alfabética. Rótulos com acento (Culinários, Lácteos, Nutrição). Médias oficiais inalteradas (43/35/36/33/37%); média simples ≈ ponderada por valor (diferença ≤ 0,2 p.p.).