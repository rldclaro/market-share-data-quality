# Matriz de rastreabilidade

Legenda: ⬜ pendente · 🟨 parcial (justificado) · ✅ feito (com evidência)

Validação final: `sql/validacao/00_pente_fino.sql` — **41/41 OK** no Databricks (pipeline completo).

## Enunciado

| # | Requisito | Seção | Status | Evidência |
|---|---|---|---|---|
| E01 | Solução em PySpark integrando todos os datasets | §3, §4 | ✅ | job `market-share-pipeline` (5 tasks: bronze → silver_dimensoes → silver_fato → gold → evidencias); `src/silver/*`, `src/gold/*`; os 7 datasets usados (o de contatos excluído por LGPD, D11) |
| E02 | Identificar problemas em dimensões e fatos | §3 | ✅ | `sql/profiling/01–06`; README › Camadas (tabelas de regras com volumes) |
| E03 | Tratamento automático só quando houver regra segura | §3, §6 | ✅ | correções com prova (PRD_005/006/007, LOJ_002/003, TER_001, CAL_001/004, FCT_007); sem prova → alerta (LOJ_004, PRD_003) ou quarentena (FCT_002/003/008/009/010); README › Q2 |
| E04 | Preservar rastreabilidade (dado original) | §3, §8 | ✅ | Bronze imutável com `_source_file`, `_row_hash`, `_load_id`; `<coluna>_raw`; `ms_dq.corrections`; payload em `ms_dq.quarantine`; README › Q3 |
| E05 | Encaminhar exceções para quarentena | §3 | ✅ | `DQEngine.quarantine()`; `ms_dq.quarantine`; fila `ms_dq.vw_quarentena_fato` |
| E06 | Uso relevante e justificado de Window | §4, §9 | ✅ | sobrevivência do EAN (`row_number`), vigência SCD2 (`lead`), duplicata/conflito (`count over`), gaps-and-islands das séries, hierarquia dominante, MS (`sum over` célula, `dense_rank`) |
| E07 | Uso relevante e justificado de GroupBy | §4, §9 | ✅ | baseline mediana/MAD por série, mediana de preço por produto, cobertura por semana × rede, MS por célula, estatísticas do motor |
| E08 | Uso relevante e justificado de Union | §4, §9 | ✅ | `unionByName(allowMissingColumns=True)` A + B (`fato.py`); MS dos 5 níveis; quarentena/trilha do motor |
| E09 | Classes / componentes reutilizáveis | §4, §9 | ✅ | `Regra`, `CatalogoRegras`, `DQEngine`; builders `ProdutoBuilder`, `LojaBuilder`, `TerritorioBuilder`, `CalendarioBuilder`, `FatoBuilder`, `CoberturaBuilder`, `MarketShareBuilder` |
| E10 | Integração fato × dimensão | §4 | ✅ | `fato.py`: joins (broadcast) com calendário, loja, produto e território as-of; FCT_004/005/006 |
| E11 | Tratamento de schemas (A ≠ B) | §4 | ✅ | mapeamento em `referencias.yml › fato.fornecedores`; ING_002 (obrigatória ausente) / ING_003 (drift) |
| E12 | Deduplicação | §4 | ✅ | FCT_001 (hash), FCT_002 (versões), FCT_003 (A × B), PRD_002 (EAN) |
| E13 | Reconciliação | §4 | ✅ | ING_001 (volumes + SHA-256), GLD_001 (conservação), GLD_002 (valor Silver = Gold = MS), COV_008, `sql/validacao/*` |
| E14 | Repo com README, dependências, premissas, execução, testes, resultados, limitações | §4 | ✅ | README: Stack, Como executar, Camadas, Monitoramento, Respostas, Insights, Limitações; testes = garantias no job + pente fino (E34) |
| E15 | Sem credenciais, tokens ou dados pessoais no repo | §4 | ✅ | `.gitignore` (segredos, `.databrickscfg`, contatos); OAuth da CLI fora do repo; e-mail do alerta via `${workspace.current_user.userName}` |
| E16 | Regras nas 9 dimensões | §5 | ✅ | `config/dq_rules.yml` (63 regras): completude, validade, unicidade, consistência, integridade referencial, acurácia, temporalidade, cobertura, reconciliação |
| E17 | Cada regra com id, descrição, escopo, severidade, critério, resultado, volume avaliado, falhas, tratamento | §5 | ✅ | `dq_rules.yml` + `ms_dq.rule_results` (avaliados, falhas, % e R$ impactado, resultado) |
| E18 | Classificação: Aprovado / Corrigido / Alerta / Quarentena | §6 | ✅ | `Classificacao` (+ REJEITADO para descarte justificado); `dq_status` por precedência em cada linha |
| E19 | Dimensões de produto e loja tratadas | §7 | ✅ | `ms_silver.dim_produto` (1.200), `dim_loja` (2.500), `territorio_historico`, `dim_calendario` |
| E20 | Fato consolidada para Market Share | §7 | ✅ | `ms_silver.fato_vendas` (124.861) e `ms_gold.fato_vendas` |
| E21 | Quarentena com motivo, regra, severidade e origem | §7 | ✅ | `ms_dq.quarantine` (regra, classificação, severidade, dimensão, arquivo de origem, chave, motivo, R$, payload) |
| E22 | Log detalhado + métricas agregadas de DQ | §7 | ✅ | `ms_dq.rule_results` (append por execução), `vw_quarentena_fato`, `vw_revalidacao_loja` |
| E23 | MS com fórmula, granularidade e premissas | §7 | ✅ | README › Gold (fórmula, 5 recortes, mesma base, valor × volume); `ms_gold.market_share` |
| E24 | Monitoramento: status, tendência, falhas, cobertura, investigação | §7 | ✅ | README › Monitoramento; `rule_results` (tendência), `ms_status`, `vw_cobertura_fornecedor`, filas de tratativa, alerta por e-mail |
| E25 | Q1 Principais problemas e priorização | §8 | ✅ | README › Respostas › 1 |
| E26 | Q2 Seguro automatizar × análise manual | §8 | ✅ | README › Respostas › 2 |
| E27 | Q3 Dado original e rastreabilidade | §8 | ✅ | README › Respostas › 3 |
| E28 | Q4 Ausência de venda × ausência de cobertura | §8 | ✅ | README › Respostas › 4; FCT_016; `cobertura_fornecedor` |
| E29 | Q5 Dados incompletos não distorcerem o MS | §8 | ✅ | README › Respostas › 5; GLD_004 |
| E30 | Q6 Integridade, idempotência, reprocessamento | §8 | ✅ | README › Respostas › 6; `replaceWhere` por alvo; GLD_001/002 |
| E31 | Q7 Mudança de schema e produção | §8 | ✅ | README › Respostas › 7; ING_002/003; Asset Bundle |
| E32 | Q8 Limitações e riscos | §8 | ✅ | README › Limitações e riscos |
| E33 | Dados tratados/amostras, quarentena e relatório de validações | §9 | ✅ | task `evidencias` (`notebooks/05_evidencias.py`) gera e `scripts/baixar_evidencias.sh` traz para `docs/evidencias/`: pente fino (41/41), rule_results, quarentena (resumo + amostra), MS nacional, amostra da fato, gráficos |
| E34 | Testes automatizados ou evidências de validação | §9 | ✅ | `tests/` (13 testes pytest, casos de borda) + garantias que falham o job + pente fino como última task do job (41 verificações); README › Testes e validação |
| E35 | Arquitetura de operação e monitoramento | §9 | ✅ | README › Arquitetura, Como executar (job), Monitoramento, Q7 |
| E36 | Apresentação, documento técnico e/ou notebook | §9 | ✅ | `notebooks/01–05` (markdown explicativo) + README |

## Nice to have

| # | Item | Status | Evidência |
|---|---|---|---|
| N01 | 5 insights com dados aprovados | ✅ | README › Insights |
| N02 | Biblioteca adicional justificada | ✅ | PyYAML — README › Stack |
| N03 | Estratégia para dados sensíveis | ✅ | README › Dados sensíveis; GLD_005; pente fino 77/78 |
| N04 | Visualização de dados | ✅ | `docs/evidencias/graficos/` (4 gráficos, no README › Insights) |
| N05 | Configuração externa de regras | ✅ | `config/dq_rules.yml`, `config/referencias.yml` |
| N06 | Logs estruturados | ✅ | `ms_dq.rule_results`, `quarantine`, `corrections`, `bronze_load_log` (Delta) |
| N07 | Delta Lake | ✅ | todas as camadas em Delta; `replaceWhere`; time travel da Gold |
| N08 | CI/CD | ✅ | `.github/workflows/ci_cd.yml`: pytest a cada push/PR; `bundle validate` + `deploy` na `main` após CI verde; segredos no GitHub Secrets; run verde no GitHub Actions (CI · testes + CD · deploy) |
| N09 | Processamento incremental | 🟨 | full reprocess idempotente; estratégia incremental em Próximas melhorias |
| N10 | Idempotência | ✅ | overwrite por camada, `replaceWhere` por alvo, `DECIMAL`, sem dependência de relógio |
| N11 | Schema drift | ✅ | ING_003 + Bronze toda string |
| N12 | Lineage | ✅ | `_source_file`, `_source_modified_at`, `_row_hash`, `_load_id`; `run_id` no log; Unity Catalog |
| N13 | Alertas | ✅ | `email_notifications.on_failure` no job + asserts/portões que falham a task |

## README dos dados

| # | Exigência | Status | Evidência |
|---|---|---|---|
| D01 | Não alterar arquivos de entrada manualmente | ✅ | `data/raw` imutável; SHA-256 no `bronze_load_log` |
| D02 | Correção, exclusão, inferência e quarentena reproduzíveis por código | ✅ | tudo no `DQEngine` + YAML; reexecução gera o mesmo resultado |
| D03 | Documentar hipóteses | ✅ | README › Limitações (premissas a validar), decisões em cada camada |
| D04 | Documentar granularidade | ✅ | Silver: fornecedor × semana × loja × EAN; Gold: semana × categoria × marca × recorte |
| D05 | Validar e documentar a unidade de volume | ✅ | FCT_020 (99.397 não reconciliam com a embalagem); premissa kg; MS volume secundário |
| D06 | Documentar fórmula de Market Share | ✅ | README › Gold |
| D07 | Não publicar credenciais, segredos ou dados reais | ✅ | `.gitignore`; catálogo real da Nestlé avaliado e **não** usado |
| D08 | Reconciliar volumes por arquivo | ✅ | ING_001 / `bronze_load_log`; pente fino 1–8 |
| D09 | Séries: variação aceitável × evento atípico × possível erro | ✅ | `anomaly_class` (VARIACAO_ACEITAVEL / EVENTO_ATIPICO / QUEDA_ATIPICA / POSSIVEL_ERRO); FCT_010/011/012 |
| D10 | Não depender só de limite global fixo | ✅ | baseline local por série (mediana + MAD) + duração (gaps-and-islands); faixa de preço local/produto |
| D11 | Dados sensíveis: minimização e proteção | ✅ | contatos não versionados, não ingeridos, bloqueados na Gold (GLD_005) |

## Critérios de avaliação (revisão final)

| Critério | Onde se vê |
|---|---|
| Diagnóstico | `sql/profiling/*`, README › Q1 |
| Código PySpark | `src/` (builders + motor), broadcast/Window/unionByName justificados |
| Modelagem | Medallion por schema; Silver no grão do fornecedor; Gold multinível |
| Consistência do MS | GLD_002/003; pente fino 70–76 |
| Relevância das regras | 63 regras amarradas a problemas reais medidos |
| Testes | 13 testes pytest + garantias no job + pente fino 41/41 |
| Auditabilidade | `_raw`, trilha, payload, `run_id`, log por execução |
| Escalabilidade | broadcast só no que é limitado; AQE; Delta; estratégia incremental documentada |
| Documentação / comunicação | README, notebooks, notas do projeto |
| Correção automática não segura | LOJ_004 (coordenada), PRD_003 (EAN), FCT_008/009/010, FCT_002/003 |