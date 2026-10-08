-- =============================================================================
-- 02 · Duplicatas exatas na fato (regra FCT_001)
--
-- Objetivo : encontrar linhas idênticas (mesmo _row_hash) dentro de cada fornecedor.
-- Achado   : 250 linhas excedentes em A e 250 em B (cada hash aparece 2 vezes).
-- Impacto  : a venda é contada 2x -> infla marca e denominador de forma desigual
--            -> Market Share distorcido sem sinal visível.
-- Tratamento seguro: manter 1 ocorrência (conteúdo idêntico) e rejeitar as cópias.
-- =============================================================================

-- 2.1 Detalhe: quais hashes se repetem (fornecedor A)
SELECT _row_hash, COUNT(*) AS copias
FROM workspace.ms_bronze.fact_provider_a
GROUP BY _row_hash
HAVING COUNT(*) > 1
ORDER BY copias DESC;

-- 2.2 Resumo: quantas linhas excedentes em cada fornecedor
SELECT 'A' AS fornecedor,
       COUNT(*)        AS hashes_repetidos,
       SUM(copias - 1) AS linhas_excedentes        -- 1 é legítima, o resto é cópia
FROM (SELECT _row_hash, COUNT(*) AS copias
      FROM workspace.ms_bronze.fact_provider_a
      GROUP BY _row_hash HAVING COUNT(*) > 1)
UNION ALL
SELECT 'B',
       COUNT(*),
       SUM(copias - 1)
FROM (SELECT _row_hash, COUNT(*) AS copias
      FROM workspace.ms_bronze.fact_provider_b
      GROUP BY _row_hash HAVING COUNT(*) > 1);