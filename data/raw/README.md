# Case Data Engineering Specialist GF7 - Data Quality

Todos os dados deste pacote sao sinteticos e foram criados exclusivamente para avaliacao tecnica.

## Regras de uso
- Nao altere os arquivos de entrada manualmente.
- Toda correcao, exclusao, inferencia e quarentena deve ser reproduzivel por codigo.
- Documente hipoteses, granularidade, unidade final de volume e formula de Market Share.
- Nao publique credenciais, tokens, segredos ou dados reais no repositorio.

## Arquivos e volumes
- dim_produto.csv: 1.235 linhas
- dim_loja.csv: 2.500 linhas
- dim_calendario.csv: 91 linhas
- customer_territory_history.csv: 4.736 linhas
- fact_market_share_provider_a.csv: 68.526 linhas
- fact_market_share_provider_b.csv: 58.526 linhas
- coverage_provider.csv: 6.370 linhas
- sensitive_store_contacts.csv: 400 linhas

As duas tabelas fato possuem juntas 127.052 registros.

## Campos padronizados nas fatos
- sales_value_brl: valor de venda em reais.
- sold_volume: volume fisico vendido. A unidade final adotada deve ser validada e documentada.

Existem series temporais por loja com comportamento historico recorrente e alteracoes intencionais de magnitude e duracao. O candidato deve distinguir variacao aceitavel, evento atipico e possivel erro, evitando depender apenas de um limite global fixo.

## Observacao
O arquivo sensitive_store_contacts.csv tambem e sintetico e foi incluido apenas para permitir a discussao de minimizacao e protecao de dados.
