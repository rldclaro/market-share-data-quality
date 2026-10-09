-- =============================================================================
-- PENTE FINO · validação ponta a ponta (Bronze -> Silver -> DQ -> Gold)
-- Uma linha por verificação, com esperado x obtido. Rodar depois do job completo.
-- Critério de entrega: TODAS as linhas com status = 'OK'.
-- =============================================================================

WITH
ultimo_run AS (   -- todas as tasks de uma execução do job gravam o mesmo run_id ({{job.run_id}})
  SELECT max_by(run_id, executado_em) AS run_id FROM workspace.ms_dq.rule_results WHERE alvo = 'cobertura'
),
log AS (
  SELECT r.* FROM workspace.ms_dq.rule_results r JOIN ultimo_run u ON r.run_id = u.run_id
),
checks AS (

  -- BRONZE: volumes do README do case
  SELECT 1 AS ordem, 'Bronze' AS camada, 'dim_produto: linhas' AS verificacao, 1235 AS esperado,
         (SELECT COUNT(*) FROM workspace.ms_bronze.dim_produto) AS obtido
  UNION ALL SELECT 2, 'Bronze', 'dim_loja: linhas', 2500, (SELECT COUNT(*) FROM workspace.ms_bronze.dim_loja)
  UNION ALL SELECT 3, 'Bronze', 'dim_calendario: linhas', 91, (SELECT COUNT(*) FROM workspace.ms_bronze.dim_calendario)
  UNION ALL SELECT 4, 'Bronze', 'customer_territory_history: linhas', 4736,
                   (SELECT COUNT(*) FROM workspace.ms_bronze.customer_territory_history)
  UNION ALL SELECT 5, 'Bronze', 'coverage_provider: linhas', 6370, (SELECT COUNT(*) FROM workspace.ms_bronze.coverage_provider)
  UNION ALL SELECT 6, 'Bronze', 'fact_provider_a: linhas', 68526, (SELECT COUNT(*) FROM workspace.ms_bronze.fact_provider_a)
  UNION ALL SELECT 7, 'Bronze', 'fact_provider_b: linhas', 58526, (SELECT COUNT(*) FROM workspace.ms_bronze.fact_provider_b)
  UNION ALL SELECT 8, 'Bronze', 'arquivos divergentes do README na última carga (ING_001)', 0,
                   (SELECT COUNT_IF(status <> 'APROVADO') FROM workspace.ms_dq.bronze_load_log
                    WHERE load_id = (SELECT max_by(load_id, loaded_at) FROM workspace.ms_dq.bronze_load_log))

  -- SILVER: dimensões
  UNION ALL SELECT 10, 'Silver', 'dim_produto: EAN duplicado', 0,
                   (SELECT COUNT(*) - COUNT(DISTINCT ean) FROM workspace.ms_silver.dim_produto)
  UNION ALL SELECT 11, 'Silver', 'dim_produto: produtos (1 por EAN)', 1200, (SELECT COUNT(*) FROM workspace.ms_silver.dim_produto)
  UNION ALL SELECT 12, 'Silver', 'dim_produto: categoria vazia', 0,
                   (SELECT COUNT_IF(category IS NULL OR category = '') FROM workspace.ms_silver.dim_produto)
  UNION ALL SELECT 13, 'Silver', 'dim_loja: store_id duplicado', 0,
                   (SELECT COUNT(*) - COUNT(DISTINCT store_id) FROM workspace.ms_silver.dim_loja)
  UNION ALL SELECT 14, 'Silver', 'dim_loja: UF fora do domínio', 0,
                   (SELECT COUNT_IF(state IS NULL OR NOT state RLIKE '^(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)$')
                    FROM workspace.ms_silver.dim_loja)
  UNION ALL SELECT 15, 'Silver', 'dim_loja: CNPJ fora de 14 dígitos', 0,
                   (SELECT COUNT_IF(NOT cnpj RLIKE '^[0-9]{14}$') FROM workspace.ms_silver.dim_loja)
  UNION ALL SELECT 16, 'Silver', 'dim_loja: coordenada alterada pelo pipeline', 0,
                   (SELECT COUNT_IF(s.latitude <> CAST(b.latitude AS DOUBLE) OR s.longitude <> CAST(b.longitude AS DOUBLE))
                    FROM workspace.ms_silver.dim_loja s JOIN workspace.ms_bronze.dim_loja b USING (store_id))
  UNION ALL SELECT 17, 'Silver', 'territorio: vigências sobrepostas', 0,
                   (SELECT COUNT(*) FROM (
                      SELECT valid_to, LEAD(valid_from) OVER (PARTITION BY store_id, category ORDER BY valid_from) AS prox
                      FROM workspace.ms_silver.territorio_historico) WHERE prox IS NOT NULL AND valid_to >= prox)
  UNION ALL SELECT 18, 'Silver', 'calendario: semanas', 91, (SELECT COUNT(*) FROM workspace.ms_silver.dim_calendario)
  UNION ALL SELECT 19, 'Silver', 'calendario: ano/mês fora do ISO', 0,
                   (SELECT COUNT_IF(year <> year(date_add(week_start_date, 3)) OR month <> month(date_add(week_start_date, 3)))
                    FROM workspace.ms_silver.dim_calendario)

  -- SILVER: fato
  UNION ALL SELECT 30, 'Silver', 'fato: conservação Bronze - (Silver + quarentena)', 0,
                   (SELECT (SELECT COUNT(*) FROM workspace.ms_bronze.fact_provider_a)
                         + (SELECT COUNT(*) FROM workspace.ms_bronze.fact_provider_b)
                         - (SELECT COUNT(*) FROM workspace.ms_silver.fato_vendas)
                         - (SELECT COUNT(*) FROM workspace.ms_dq.quarantine WHERE alvo = 'fato_vendas'))
  UNION ALL SELECT 31, 'Silver', 'fato: chave semana x loja x EAN duplicada', 0,
                   (SELECT COUNT(*) - COUNT(DISTINCT year_week, store_id, ean) FROM workspace.ms_silver.fato_vendas)
  UNION ALL SELECT 32, 'Silver', 'fato: valor negativo', 0,
                   (SELECT COUNT_IF(sales_value_brl < 0) FROM workspace.ms_silver.fato_vendas)
  UNION ALL SELECT 33, 'Silver', 'fato: loja fora da dim_loja', 0,
                   (SELECT COUNT(*) FROM workspace.ms_silver.fato_vendas LEFT ANTI JOIN workspace.ms_silver.dim_loja USING (store_id))
  UNION ALL SELECT 34, 'Silver', 'fato: EAN fora da dim_produto', 0,
                   (SELECT COUNT(*) FROM workspace.ms_silver.fato_vendas LEFT ANTI JOIN workspace.ms_silver.dim_produto USING (ean))
  UNION ALL SELECT 35, 'Silver', 'fato: linha em quarentena/rejeitada na Silver', 0,
                   (SELECT COUNT_IF(dq_status IN ('QUARENTENA', 'REJEITADO')) FROM workspace.ms_silver.fato_vendas)
  UNION ALL SELECT 36, 'Silver', 'cobertura: chave fornecedor x semana x rede duplicada', 0,
                   (SELECT COUNT(*) - COUNT(DISTINCT provider, year_week, retailer_name) FROM workspace.ms_silver.cobertura_fornecedor)
  UNION ALL SELECT 37, 'Silver', 'cobertura: acima de 100%', 0,
                   (SELECT COUNT_IF(coverage_ratio > 1) FROM workspace.ms_silver.cobertura_fornecedor)

  -- DQ: log x trilha x quarentena (última execução)
  -- 63 regras no catálogo; a ING_001 (reconciliação da Bronze) é registrada em ms_dq.bronze_load_log (check 8)
  UNION ALL SELECT 50, 'DQ', 'regras no log da última execução', 62, (SELECT COUNT(DISTINCT regra_id) FROM log)
  UNION ALL SELECT 51, 'DQ', 'correções: |falhas no log - registros na trilha|', 0,
                   (SELECT COALESCE(SUM(ABS(l.falhas - COALESCE(t.registros, 0))), 0)
                    FROM log l
                    LEFT JOIN (SELECT regra_id, alvo, COUNT(DISTINCT chave) AS registros
                               FROM workspace.ms_dq.corrections GROUP BY ALL) t USING (regra_id, alvo)
                    WHERE l.resultado = 'CORRIGIDO_AUTOMATICAMENTE')
  UNION ALL SELECT 52, 'DQ', 'quarentena: |falhas no log - linhas na quarentena|', 0,
                   (SELECT COALESCE(SUM(ABS(l.falhas - COALESCE(q.linhas, 0))), 0)
                    FROM log l
                    LEFT JOIN (SELECT regra_id, alvo, COUNT(*) AS linhas
                               FROM workspace.ms_dq.quarantine GROUP BY ALL) q USING (regra_id, alvo)
                    WHERE l.resultado IN ('QUARENTENA', 'REJEITADO') AND l.regra_id NOT LIKE 'GLD%'
                      AND l.regra_id NOT IN ('ING_002'))
  UNION ALL SELECT 53, 'DQ', 'quarentena sem motivo, regra ou payload', 0,
                   (SELECT COUNT_IF(motivo IS NULL OR regra_id IS NULL OR payload IS NULL) FROM workspace.ms_dq.quarantine)
  UNION ALL SELECT 54, 'DQ', 'correção sem valor antes/depois', 0,
                   (SELECT COUNT_IF(coluna IS NULL OR chave IS NULL) FROM workspace.ms_dq.corrections)

  -- GOLD
  UNION ALL SELECT 70, 'Gold', 'linhas: Silver consumível - Gold', 0,
                   (SELECT (SELECT COUNT_IF(dq_status IN ('APROVADO', 'CORRIGIDO_AUTOMATICAMENTE', 'APROVADO_COM_ALERTA'))
                            FROM workspace.ms_silver.fato_vendas)
                         - (SELECT COUNT(*) FROM workspace.ms_gold.fato_vendas))
  UNION ALL SELECT 71, 'Gold', 'valor (centavos): |Σ Gold - Σ MS nacional|', 0,
                   (SELECT CAST(ROUND(ABS((SELECT SUM(sales_value_brl) FROM workspace.ms_gold.fato_vendas)
                                        - (SELECT SUM(brand_value_brl) FROM workspace.ms_gold.market_share
                                           WHERE level = 'NACIONAL')) * 100) AS BIGINT))
  UNION ALL SELECT 72, 'Gold', 'células com Σ shares ≠ 100%', 0,
                   (SELECT COUNT(*) FROM (
                      SELECT level, level_value, year_week, category, SUM(ms_value) AS s
                      FROM workspace.ms_gold.market_share WHERE ms_value IS NOT NULL GROUP BY ALL)
                    WHERE ABS(s - 1) > 0.0001)
  UNION ALL SELECT 73, 'Gold', 'oficial com cobertura < 80% ou quarentena > 5%', 0,
                   (SELECT COUNT_IF(ms_status = 'OFICIAL' AND (coverage_ratio < 0.80 OR quarantine_row_pct > 0.05))
                    FROM workspace.ms_gold.market_share)
  UNION ALL SELECT 74, 'Gold', 'semana aberta (2026-40) publicada como oficial', 0,
                   (SELECT COUNT_IF(ms_status = 'OFICIAL' AND year_week = '2026-40') FROM workspace.ms_gold.market_share)
  UNION ALL SELECT 75, 'Gold', 'não oficial com valor oficial preenchido', 0,
                   (SELECT COUNT_IF(ms_status <> 'OFICIAL' AND ms_value_official IS NOT NULL) FROM workspace.ms_gold.market_share)
  UNION ALL SELECT 76, 'Gold', 'view oficial - células OFICIAL', 0,
                   (SELECT (SELECT COUNT(*) FROM workspace.ms_gold.vw_market_share_oficial)
                         - (SELECT COUNT_IF(ms_status = 'OFICIAL') FROM workspace.ms_gold.market_share))
  UNION ALL SELECT 77, 'Gold', 'colunas pessoais/sensíveis na Gold (LGPD)', 0,
                   (SELECT COUNT(*) FROM workspace.information_schema.columns
                    WHERE table_schema = 'ms_gold'
                      AND column_name IN ('contact_name', 'contact_email', 'contact_phone', 'cnpj', 'latitude', 'longitude'))
  UNION ALL SELECT 78, 'Gold', 'tabela de contatos sensíveis no catálogo (LGPD)', 0,
                   (SELECT COUNT(*) FROM workspace.information_schema.tables
                    WHERE table_schema LIKE 'ms_%' AND table_name LIKE '%contact%')
  UNION ALL SELECT 79, 'Gold', 'portões críticos (GLD_001/002/003/005) com falha', 0,
                   (SELECT COUNT_IF(falhas > 0) FROM log WHERE regra_id IN ('GLD_001', 'GLD_002', 'GLD_003', 'GLD_005'))
)
SELECT camada, verificacao, esperado, obtido,
       CASE WHEN obtido = esperado THEN 'OK' ELSE 'FALHA' END AS status
FROM checks
ORDER BY ordem;