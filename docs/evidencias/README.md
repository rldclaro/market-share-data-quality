# Evidências

Resultados do pipeline para revisão sem acesso ao workspace. Dados sintéticos do case, agregados ou
amostrados; nenhum dado pessoal (o arquivo de contatos não é ingerido).

| Arquivo | Conteúdo |
|---|---|
| `pente_fino.csv` | resultado de `sql/validacao/00_pente_fino.sql` **no Databricks**: 41/41 OK |
| `rule_results.csv` | uma linha por regra × alvo: avaliados, falhas, % e R$ impactado, resultado e detalhes |
| `quarentena_resumo.csv` | quarentena e rejeitados por regra: linhas, R$ retido e exemplo de motivo |
| `quarentena_amostra.csv` | até 5 registros por regra da fato, com chave, arquivo de origem e motivo |
| `market_share_nacional.csv` | Market Share nacional completo (todas as semanas, com status de publicação) |
| `fato_vendas_amostra.csv` | 500 linhas da fato consumível da última semana oficial |
| `graficos/` | os 4 gráficos dos insights do README |

Gerados pela task `evidencias` do job (`notebooks/05_evidencias.py` + `src/relatorios/evidencias.py`)
a partir das tabelas publicadas, gravados no Volume `ms_dq.evidencias` e trazidos para cá com
`./scripts/baixar_evidencias.sh`. Para atualizar: rodar o job e o script.