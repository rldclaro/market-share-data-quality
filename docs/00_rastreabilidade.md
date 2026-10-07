# Matriz de rastreabilidade

Legenda: ⬜ pendente · 🟨 em andamento · ✅ feito (com link da evidência)

## Enunciado

| # | Requisito | Seção | Etapa | Status | Evidência |
|---|---|---|---|---|---|
| E01 | Solução em PySpark integrando todos os datasets | §3, §4 | 3–7 | ⬜ | |
| E02 | Identificar problemas em dimensões e fatos | §3 | 3–6 | ⬜ | |
| E03 | Tratamento automático só quando houver regra segura | §3, §6 | 4–6 | ⬜ | |
| E04 | Preservar rastreabilidade (dado original) | §3, §8 | 3–6 | ⬜ | |
| E05 | Encaminhar exceções para quarentena | §3 | 4–6 | ⬜ | |
| E06 | Uso relevante e justificado de Window | §4, §9 | 5–7 | ⬜ | |
| E07 | Uso relevante e justificado de GroupBy | §4, §9 | 6–7 | ⬜ | |
| E08 | Uso relevante e justificado de Union | §4, §9 | 6 | ⬜ | |
| E09 | Classes / componentes reutilizáveis | §4, §9 | 4 | ⬜ | |
| E10 | Integração fato × dimensão | §4 | 6 | ⬜ | |
| E11 | Tratamento de schemas (A ≠ B) | §4 | 6 | ⬜ | |
| E12 | Deduplicação | §4 | 5–6 | ⬜ | |
| E13 | Reconciliação | §4 | 3, 7 | ⬜ | |
| E14 | Repo com README, dependências, premissas, execução, testes, resultados, limitações | §4 | 9 | ⬜ | |
| E15 | Sem credenciais, tokens ou dados pessoais no repo | §4 | 0–9 | ⬜ | |
| E16 | Regras nas 9 dimensões (completude, validade, unicidade, consistência, integridade referencial, acurácia, temporalidade, cobertura, reconciliação) | §5 | 4–7 | ⬜ | |
| E17 | Cada regra com id, descrição, escopo, severidade, critério, resultado, volume avaliado, falhas, tratamento | §5 | 4 | ⬜ | |
| E18 | Classificação: Aprovado / Corrigido automaticamente / Aprovado com alerta / Quarentena | §6 | 4 | ⬜ | |
| E19 | Dimensões de produto e loja tratadas | §7 | 5 | ⬜ | |
| E20 | Fato consolidada para Market Share | §7 | 6 | ⬜ | |
| E21 | Quarentena com motivo, regra, severidade e origem | §7 | 4–6 | ⬜ | |
| E22 | Log detalhado + métricas agregadas de DQ | §7 | 4, 8 | ⬜ | |
| E23 | MS com fórmula, granularidade e premissas | §7 | 7, 9 | ⬜ | |
| E24 | Monitoramento: status, tendência, falhas, cobertura, investigação | §7 | 8 | ⬜ | |
| E25 | Q1 Principais problemas e priorização | §8 | 9 | ⬜ | |
| E26 | Q2 Seguro automatizar × análise manual | §8 | 9 | ⬜ | |
| E27 | Q3 Dado original e rastreabilidade | §8 | 9 | ⬜ | |
| E28 | Q4 Ausência de venda × ausência de cobertura | §8 | 7, 9 | ⬜ | |
| E29 | Q5 Dados incompletos não distorcerem o MS | §8 | 7, 9 | ⬜ | |
| E30 | Q6 Integridade, idempotência, reprocessamento | §8 | 6–9 | ⬜ | |
| E31 | Q7 Mudança de schema e produção | §8 | 8, 9 | ⬜ | |
| E32 | Q8 Limitações e riscos | §8 | 9 | ⬜ | |
| E33 | Dados tratados/amostras, quarentena e relatório de validações | §9 | 8 | ⬜ | |
| E34 | Testes automatizados ou evidências de validação | §9 | 8 | ⬜ | |
| E35 | Arquitetura de operação e monitoramento | §9 | 9 | ⬜ | |
| E36 | Apresentação, documento técnico e/ou notebook | §9 | 2–9 | ⬜ | |

## Nice to have

| # | Item | Etapa | Status | Evidência |
|---|---|---|---|---|
| N01 | 5 insights com dados aprovados | 8 | ⬜ | |
| N02 | Biblioteca adicional justificada | 4 ou 8 | ⬜ | |
| N03 | Estratégia para dados sensíveis | 1, 9 | ⬜ | |
| N04 | Visualização de dados | 8 | ⬜ | |
| N05 | Configuração externa de regras | 4 | ⬜ | |
| N06 | Logs estruturados | 4 | ⬜ | |
| N07 | Delta Lake | 3 | ⬜ | |
| N08 | CI/CD | 8 | ⬜ | |
| N09 | Processamento incremental | 8–9 | ⬜ | |
| N10 | Idempotência | 3–8 | ⬜ | |
| N11 | Schema drift | 6 | ⬜ | |
| N12 | Lineage | 3 | ⬜ | |
| N13 | Alertas | 8 | ⬜ | |

## README dos dados

| # | Exigência | Etapa | Status | Evidência |
|---|---|---|---|---|
| D01 | Não alterar arquivos de entrada manualmente | 1, 3 | ⬜ | |
| D02 | Correção, exclusão, inferência e quarentena reproduzíveis por código | 3–7 | ⬜ | |
| D03 | Documentar hipóteses | 9 | ⬜ | |
| D04 | Documentar granularidade | 7, 9 | ⬜ | |
| D05 | Validar e documentar a unidade de volume (`sold_volume`) | 6, 9 | ⬜ | |
| D06 | Documentar fórmula de Market Share | 7, 9 | ⬜ | |
| D07 | Não publicar credenciais, segredos ou dados reais | 0–9 | ⬜ | |
| D08 | Reconciliar volumes por arquivo | 3 | ⬜ | |
| D09 | Séries: variação aceitável × evento atípico × possível erro | 6 | ⬜ | |
| D10 | Não depender só de limite global fixo | 6 | ⬜ | |
| D11 | Dados sensíveis: minimização e proteção | 1, 9 | ⬜ | |

## Critérios de avaliação (revisão final)

Diagnóstico · código PySpark · modelagem · consistência do MS · relevância das regras · testes ·
auditabilidade · escalabilidade · documentação · comunicação · reconhecer quando a correção
automática não é segura.
