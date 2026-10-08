-- =============================================================================
-- 03 · Mesma chave com versões conflitantes (regra FCT_002)
--
-- Objetivo : encontrar a mesma chave de negócio (semana x loja x EAN) com valores
--            diferentes, depois de descontar as duplicatas exatas.
-- Achado   : 11 chaves em A (22 linhas) e 5 em B (10 linhas).
-- Evidências:
--   - o grão é agregado semanal: 1 linha por chave por definição (não são "movimentações");
--   - a maioria das versões vem do MESMO arquivo, carregado em datas diferentes;
--   - preço médio incoerente entre versões (ex.: R$ 37,04 vs R$ 6,56 na mesma semana/loja/produto);
--   - 2 casos em A e 1 em B têm uma versão no arquivo provider_*_series_* (segunda fonte).
-- Tratamento: QUARENTENA de todas as versões (sem número de versão, a carga mais
--             recente não prova ser a correta). Somar contaria em dobro.
-- Bônus    : em B aparece o EAN malformado "EAN-00000635" (ver na Etapa 5).
-- =============================================================================

-- 3.1 Fornecedor A: uma linha por versão, com valor e data de carga lado a lado
WITH chaves_conflitantes AS (
  SELECT week, store_id, ean
  FROM workspace.ms_bronze.fact_provider_a
  GROUP BY week, store_id, ean
  HAVING COUNT(DISTINCT _row_hash) > 1
)
SELECT f.week, f.store_id, f.ean,
       to_timestamp(f.ingestion_timestamp)                       AS carga,   -- texto -> timestamp para ordenar certo
       f.units, f.sold_volume, f.sales_value_brl,
       ROUND(f.sales_value_brl / NULLIF(f.units, 0), 2)          AS preco_medio,
       f.source_file,
       ROW_NUMBER() OVER (PARTITION BY f.week, f.store_id, f.ean
                          ORDER BY to_timestamp(f.ingestion_timestamp)) AS versao   -- 1 = mais antiga
FROM workspace.ms_bronze.fact_provider_a f
JOIN chaves_conflitantes c USING (week, store_id, ean)
ORDER BY f.week, f.store_id, f.ean, versao;

-- 3.2 Fornecedor B: mesma lógica, com os nomes de coluna de B
WITH chaves_conflitantes AS (
  SELECT reference_week, customer_code, product_ean
  FROM workspace.ms_bronze.fact_provider_b
  GROUP BY reference_week, customer_code, product_ean
  HAVING COUNT(DISTINCT _row_hash) > 1
)
SELECT f.reference_week, f.customer_code, f.product_ean,
       to_timestamp(f.load_date)                                 AS carga,
       f.unit_count, f.sold_volume, f.sales_value_brl,
       ROUND(f.sales_value_brl / NULLIF(f.unit_count, 0), 2)     AS preco_medio,
       f.source_file,
       ROW_NUMBER() OVER (PARTITION BY f.reference_week, f.customer_code, f.product_ean
                          ORDER BY to_timestamp(f.load_date))    AS versao
FROM workspace.ms_bronze.fact_provider_b f
JOIN chaves_conflitantes c USING (reference_week, customer_code, product_ean)
ORDER BY 1, 2, 3, versao;