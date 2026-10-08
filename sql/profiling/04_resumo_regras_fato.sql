-- =============================================================================
-- 04 · Resumo: uma regra, vários alvos (FCT_001 e FCT_002 em A e B)
--
-- Mostra que o ID identifica a REGRA, não a base: A e B passam pelas mesmas regras,
-- e o que muda no log é a coluna "alvo".
-- O bloco "fatos" já é, em miniatura, a harmonização de schemas + Union da Silver:
-- renomeia as colunas de B para os mesmos nomes de A e junta com UNION ALL.
--
-- Esperado:
--   FCT_001 | fact_provider_a | 68.526 | 250
--   FCT_001 | fact_provider_b | 58.526 | 250
--   FCT_002 | fact_provider_a | 68.276 |  22   <- avalia menos: as duplicatas já saíram
--   FCT_002 | fact_provider_b | 58.276 |  10
-- =============================================================================

WITH fatos AS (
  SELECT 'fact_provider_a' AS alvo, _row_hash, week AS semana, store_id AS loja, ean
  FROM workspace.ms_bronze.fact_provider_a
  UNION ALL
  SELECT 'fact_provider_b', _row_hash, reference_week, customer_code, product_ean
  FROM workspace.ms_bronze.fact_provider_b
),
fct_001 AS (
  SELECT 'FCT_001' AS regra_id, 'duplicata exata' AS regra, alvo,
         COUNT(*) AS avaliados,
         COUNT(*) - COUNT(DISTINCT _row_hash) AS falhas
  FROM fatos GROUP BY alvo
),
sem_duplicatas AS (
  SELECT DISTINCT alvo, _row_hash, semana, loja, ean FROM fatos
),
fct_002 AS (
  SELECT 'FCT_002', 'versões conflitantes', alvo,
         SUM(n) AS avaliados,
         SUM(CASE WHEN n > 1 THEN n ELSE 0 END) AS falhas
  FROM (SELECT alvo, semana, loja, ean, COUNT(*) AS n
        FROM sem_duplicatas GROUP BY alvo, semana, loja, ean)
  GROUP BY alvo
)
SELECT * FROM fct_001
UNION ALL
SELECT * FROM fct_002
ORDER BY regra_id, alvo;