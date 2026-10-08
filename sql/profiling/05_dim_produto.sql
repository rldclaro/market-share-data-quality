-- =============================================================================
-- 05 · Profiling da dimensão de produto (regras PRD_*)
--
-- Achados:
--   5.1  35 EANs duplicados no cadastro (o EAN é a chave do join com a fato)
--   5.2  categorias com grafia diferente (CHOCOLATE, LACTEO), vazias (3) e genéricas (OUTROS, 21)
--   5.3  hierarquia subcategoria -> categoria seguida por ~1.200 dos 1.235 produtos:
--          CAFE -> BEBIDAS | CEREAIS -> NUTRICAO | LEITE EM PO -> LACTEOS
--          TABLETES -> CHOCOLATES | TEMPEROS -> CULINARIOS
--        conflitos com categoria válida porém errada (ex.: CEREAIS em BEBIDAS)
--   5.4  31 EANs fora do padrão de 13 dígitos: 12 dígitos sem o "7" inicial (891...)
--        e prefixo de texto (EAN-000...); descrição em minúsculas/espaços sobrando
--   5.5  o EAN malformado existe IGUAL no cadastro e na fato -> o join funciona
--        com o código "errado"; corrigir só um lado quebraria a integridade
-- =============================================================================

-- 5.1 O EAN é único no cadastro?
SELECT ean, COUNT(*) AS registros,
       COLLECT_LIST(product_id)          AS ids,
       COLLECT_LIST(category)            AS categorias,
       COLLECT_LIST(product_description) AS descricoes
FROM workspace.ms_bronze.dim_produto
GROUP BY ean
HAVING COUNT(*) > 1;

-- 5.2 Quais categorias existem? (grafia e vazios)
SELECT category, COUNT(*) AS produtos
FROM workspace.ms_bronze.dim_produto
GROUP BY category
ORDER BY produtos DESC;

-- 5.3 Categoria x subcategoria: a hierarquia é consistente?
SELECT subcategory, category, COUNT(*) AS produtos
FROM workspace.ms_bronze.dim_produto
GROUP BY subcategory, category
ORDER BY subcategory, produtos DESC;

-- 5.4 EANs fora do padrão (13 dígitos numéricos)
SELECT product_id, ean, product_description
FROM workspace.ms_bronze.dim_produto
WHERE NOT ean RLIKE '^[0-9]{13}$'
ORDER BY product_id;

-- 5.5 O EAN malformado existe igual no cadastro E na fato? (corrigir só um lado quebra o join)
SELECT 'dim_produto'     AS origem, COUNT(*) AS linhas FROM workspace.ms_bronze.dim_produto     WHERE ean         = 'EAN-00000635'
UNION ALL
SELECT 'fact_provider_a',           COUNT(*)           FROM workspace.ms_bronze.fact_provider_a WHERE ean         = 'EAN-00000635'
UNION ALL
SELECT 'fact_provider_b',           COUNT(*)           FROM workspace.ms_bronze.fact_provider_b WHERE product_ean = 'EAN-00000635';

-- 5.6 Visão geral: quantas linhas da fato usam EAN malformado (impacto se o join quebrasse)
SELECT 'A' AS fornecedor, COUNT(*) AS linhas_com_ean_malformado
FROM workspace.ms_bronze.fact_provider_a WHERE NOT ean RLIKE '^[0-9]{13}$'
UNION ALL
SELECT 'B', COUNT(*)
FROM workspace.ms_bronze.fact_provider_b WHERE NOT product_ean RLIKE '^[0-9]{13}$';