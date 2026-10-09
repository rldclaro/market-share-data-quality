-- =============================================================================
-- Validação · Silver · fato de vendas (A + B)
-- Rodar depois da task silver_fato. Tabelas: ms_bronze.fact_provider_*, ms_silver.fato_vendas, ms_dq.*
-- =============================================================================

-- 1. CONSERVAÇÃO (nada some em silêncio)
-- 1.1 Bronze = Silver + quarentena + rejeitados (diferenca deve ser 0)
SELECT b.linhas_bronze, s.linhas_silver, q.retiradas, b.linhas_bronze - s.linhas_silver - q.retiradas AS diferenca
FROM (SELECT (SELECT COUNT(*) FROM workspace.ms_bronze.fact_provider_a)
           + (SELECT COUNT(*) FROM workspace.ms_bronze.fact_provider_b) AS linhas_bronze) b,
     (SELECT COUNT(*) AS linhas_silver FROM workspace.ms_silver.fato_vendas) s,
     (SELECT COUNT(*) AS retiradas FROM workspace.ms_dq.quarantine WHERE alvo = 'fato_vendas') q;

-- 1.2 O mesmo, em valor (R$) por fornecedor
SELECT provider, linhas, valor_brl FROM (
  SELECT 'A' AS provider, COUNT(*) AS linhas, SUM(CAST(sales_value_brl AS DECIMAL(18,2))) AS valor_brl, 1 AS o
  FROM workspace.ms_bronze.fact_provider_a
  UNION ALL
  SELECT 'B', COUNT(*), SUM(CAST(sales_value_brl AS DECIMAL(18,2))), 2 FROM workspace.ms_bronze.fact_provider_b
) ORDER BY o;

-- 2. LOG DA ÚLTIMA EXECUÇÃO
SELECT regra_id, avaliados, falhas, pct_falha, valor_impactado_brl, resultado, detalhes
FROM workspace.ms_dq.rule_results
WHERE run_id = (SELECT max_by(run_id, executado_em) FROM workspace.ms_dq.rule_results WHERE alvo = 'fato_vendas')
  AND (alvo = 'fato_vendas' OR alvo LIKE 'fornecedor_%')
ORDER BY regra_id;

-- 3. QUARENTENA E REJEITADOS
-- 3.1 Por regra e fornecedor, com valor
SELECT regra_id, classificacao, get_json_object(chave, '$.provider') AS provider,
       COUNT(*) AS linhas, ROUND(SUM(valor_brl), 2) AS valor_brl
FROM workspace.ms_dq.quarantine
WHERE alvo = 'fato_vendas'
GROUP BY ALL
ORDER BY regra_id, provider;

-- 3.2 Reconciliação: linhas na quarentena = falhas no log, por regra (diferenca deve ser 0)
WITH ultimo AS (
  SELECT regra_id, falhas FROM workspace.ms_dq.rule_results
  WHERE run_id = (SELECT max_by(run_id, executado_em) FROM workspace.ms_dq.rule_results WHERE alvo = 'fato_vendas')
    AND alvo = 'fato_vendas' AND resultado IN ('QUARENTENA', 'REJEITADO')
)
SELECT u.regra_id, u.falhas AS falhas_no_log, COUNT(q.regra_id) AS linhas_na_quarentena,
       u.falhas - COUNT(q.regra_id) AS diferenca
FROM ultimo u LEFT JOIN workspace.ms_dq.quarantine q ON q.regra_id = u.regra_id AND q.alvo = 'fato_vendas'
GROUP BY u.regra_id, u.falhas
ORDER BY u.regra_id;

-- 3.3 Exemplos de motivo (preço fora da faixa e pico na série)
SELECT regra_id, chave, motivo, valor_brl
FROM workspace.ms_dq.quarantine
WHERE alvo = 'fato_vendas' AND regra_id IN ('FCT_009', 'FCT_010')
ORDER BY regra_id, valor_brl DESC
LIMIT 20;

-- 4. CORREÇÕES
-- 4.1 Sinal invertido (FCT_007): a prova é unidades x preço médio = -valor original
SELECT provider, year_week, store_id, ean, units, average_price,
       ROUND(units * average_price, 2) AS unidades_x_preco,
       sales_value_brl_raw AS valor_origem, sales_value_brl AS valor_silver
FROM workspace.ms_silver.fato_vendas
WHERE array_contains(dq_flags, 'FCT_007')
ORDER BY year_week, store_id
LIMIT 20;

-- 4.2 Trilha x log da FCT_007 (diferenca deve ser 0)
SELECT (SELECT COUNT(*) FROM workspace.ms_dq.corrections WHERE regra_id = 'FCT_007') AS na_trilha,
       (SELECT COUNT(*) FROM workspace.ms_silver.fato_vendas WHERE array_contains(dq_flags, 'FCT_007')) AS na_silver;

-- 5. ALERTAS
-- 5.1 Distribuição dos alertas (uma linha pode ter mais de um)
SELECT flag AS regra_id, provider, COUNT(*) AS linhas, ROUND(SUM(sales_value_brl), 2) AS valor_brl
FROM workspace.ms_silver.fato_vendas LATERAL VIEW explode(dq_flags) f AS flag
GROUP BY ALL
ORDER BY regra_id, provider;

-- 5.2 Eventos sustentados (FCT_011): série, duração e intensidade
SELECT provider, store_id, product_id, COUNT(*) AS semanas_em_evento,
       MIN(year_week) AS inicio, MAX(year_week) AS fim, ROUND(MAX(series_ratio), 1) AS pico_x_mediana
FROM workspace.ms_silver.fato_vendas
WHERE anomaly_class = 'EVENTO_ATIPICO'
GROUP BY ALL
ORDER BY semanas_em_evento DESC, pico_x_mediana DESC
LIMIT 20;

-- 5.3 Semana aberta na ingestão (FCT_017)
SELECT year_week, provider, COUNT(*) AS linhas, MIN(ingestion_ts) AS ingestao, MAX(week_end_date) AS fim_semana
FROM workspace.ms_silver.fato_vendas
WHERE week_open_at_ingestion
GROUP BY ALL
ORDER BY year_week, provider;

-- 5.4 Origem do território na venda (histórico as-of -> dim_loja -> SEM_TERRITORIO)
SELECT territory_source, COUNT(*) AS linhas, ROUND(SUM(sales_value_brl), 2) AS valor_brl
FROM workspace.ms_silver.fato_vendas
GROUP BY ALL
ORDER BY linhas DESC;

-- 5.5 Prova do as-of: a mesma loja x categoria muda de território na virada de 2026 (TER_001).
--     Referência da semana = quinta-feira: a 2026-01 (29/12 a 04/01) já usa a vigência de 2026
SELECT year_week, week_start_date, territory_id, territory_source, COUNT(*) AS linhas
FROM workspace.ms_silver.fato_vendas
WHERE store_id = 'S00009' AND category = 'NUTRICAO' AND year_week BETWEEN '2025-50' AND '2026-04'
GROUP BY ALL
ORDER BY year_week;

-- 6. GARANTIAS (todas devem retornar 0)
SELECT 'chave semana x loja x EAN duplicada' AS garantia,
       COUNT(*) - COUNT(DISTINCT year_week, store_id, ean) AS violacoes FROM workspace.ms_silver.fato_vendas
UNION ALL
SELECT 'valor negativo', COUNT_IF(sales_value_brl < 0) FROM workspace.ms_silver.fato_vendas
UNION ALL
SELECT 'loja fora da dim_loja', COUNT(*)
FROM workspace.ms_silver.fato_vendas f LEFT ANTI JOIN workspace.ms_silver.dim_loja d USING (store_id)
UNION ALL
SELECT 'EAN fora da dim_produto', COUNT(*)
FROM workspace.ms_silver.fato_vendas f LEFT ANTI JOIN workspace.ms_silver.dim_produto d USING (ean)
UNION ALL
SELECT 'semana fora do calendário', COUNT(*)
FROM workspace.ms_silver.fato_vendas f LEFT ANTI JOIN workspace.ms_silver.dim_calendario d USING (year_week)
UNION ALL
SELECT 'linha com status de quarentena na Silver', COUNT_IF(dq_status IN ('QUARENTENA', 'REJEITADO'))
FROM workspace.ms_silver.fato_vendas;