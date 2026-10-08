-- =============================================================================
-- Validação · Silver · dimensões (produto, loja, território)
-- Objetivo: provar que toda correção, alerta e quarentena está registrada e bate entre si.
-- Rodar depois do job (task silver_dimensoes). Tabelas: ms_silver.*, ms_dq.*
-- =============================================================================

-- ------------------------------------------------------------------ 1. LOG DA ÚLTIMA EXECUÇÃO
-- 1.1 Resultado de cada regra na última execução (rule_results é append: filtra o último run)
SELECT regra_id, alvo, severidade, avaliados, falhas, pct_falha, resultado, detalhes
FROM workspace.ms_dq.rule_results
WHERE run_id = (SELECT max_by(run_id, executado_em) FROM workspace.ms_dq.rule_results
                WHERE alvo IN ('dim_produto', 'dim_loja', 'territorio_historico'))
ORDER BY alvo, regra_id;

-- 1.2 Histórico: a mesma regra ao longo das execuções (deve ser estável com a mesma entrada)
SELECT regra_id, run_id, executado_em, falhas
FROM workspace.ms_dq.rule_results
WHERE alvo IN ('dim_produto', 'dim_loja', 'territorio_historico')
ORDER BY regra_id, executado_em DESC;

-- ------------------------------------------------------------------ 2. TRILHA DE CORREÇÕES
-- 2.1 Resumo: quantos registros e colunas cada regra corrigiu
SELECT alvo, regra_id, coluna,
       COUNT(*)              AS alteracoes,
       COUNT(DISTINCT chave) AS registros
FROM workspace.ms_dq.corrections
GROUP BY ALL
ORDER BY alvo, regra_id, coluna;

-- 2.2 Antes x depois de uma regra (troque o regra_id: LOJ_002, LOJ_003, TER_001, PRD_005, PRD_006, PRD_007...)
SELECT regra_id, chave, coluna, valor_antes, valor_depois
FROM workspace.ms_dq.corrections
WHERE regra_id = 'LOJ_002'
ORDER BY chave;

-- 2.3 Toda a história de UM registro (ex.: produto que passou por duas regras)
SELECT regra_id, coluna, valor_antes, valor_depois
FROM workspace.ms_dq.corrections
WHERE alvo = 'dim_produto' AND get_json_object(chave, '$.product_id') = 'P00334'
ORDER BY regra_id;

-- ------------------------------------------------------------------ 3. RECONCILIAÇÃO LOG x TRILHA
-- 3.1 Para toda regra de correção: registros na trilha == falhas no log (diferenca deve ser 0)
WITH ultimo AS (
  SELECT * FROM workspace.ms_dq.rule_results
  WHERE run_id = (SELECT max_by(run_id, executado_em) FROM workspace.ms_dq.rule_results
                  WHERE alvo IN ('dim_produto', 'dim_loja', 'territorio_historico'))
    AND resultado = 'CORRIGIDO_AUTOMATICAMENTE'
),
trilha AS (
  SELECT regra_id, alvo, COUNT(DISTINCT chave) AS registros
  FROM workspace.ms_dq.corrections GROUP BY ALL
)
SELECT u.alvo, u.regra_id, u.falhas AS falhas_no_log, coalesce(t.registros, 0) AS registros_na_trilha,
       u.falhas - coalesce(t.registros, 0) AS diferenca
FROM ultimo u LEFT JOIN trilha t USING (regra_id, alvo)
ORDER BY u.alvo, u.regra_id;

-- 3.2 Para toda regra de quarentena/rejeição: linhas em quarantine == falhas no log
WITH ultimo AS (
  SELECT * FROM workspace.ms_dq.rule_results
  WHERE run_id = (SELECT max_by(run_id, executado_em) FROM workspace.ms_dq.rule_results
                  WHERE alvo IN ('dim_produto', 'dim_loja', 'territorio_historico'))
    AND regra_id IN (SELECT DISTINCT regra_id FROM workspace.ms_dq.quarantine)
)
SELECT u.alvo, u.regra_id, u.resultado, u.falhas AS falhas_no_log,
       COUNT(q.regra_id) AS linhas_na_quarentena,
       u.falhas - COUNT(q.regra_id) AS diferenca
FROM ultimo u LEFT JOIN workspace.ms_dq.quarantine q USING (regra_id, alvo)
GROUP BY u.alvo, u.regra_id, u.resultado, u.falhas
ORDER BY u.alvo, u.regra_id;

-- ------------------------------------------------------------------ 4. ANTES x DEPOIS NA PRÓPRIA TABELA (<coluna>_raw)
-- 4.1 Loja · UF corrigida pela cidade (LOJ_002) — esperado 20
SELECT store_id, city, state_raw AS uf_origem, state AS uf_silver, dq_flags
FROM workspace.ms_silver.dim_loja
WHERE array_contains(dq_flags, 'LOJ_002')
ORDER BY city, store_id;

-- 4.2 Loja · CNPJ sem máscara (LOJ_003) — esperado 35
SELECT store_id, cnpj_raw AS cnpj_origem, cnpj AS cnpj_silver
FROM workspace.ms_silver.dim_loja
WHERE array_contains(dq_flags, 'LOJ_003')
ORDER BY store_id;

-- 4.3 Loja · coordenada com alerta, SEM alteração (LOJ_004) — esperado 10
SELECT store_id, city, state, latitude, longitude, geo_status, dq_status
FROM workspace.ms_silver.dim_loja
WHERE array_contains(dq_flags, 'LOJ_004')
ORDER BY store_id;

-- 4.4 Território · vigência fechada (TER_001) — esperado 121, todas com valid_to = 2025-12-31
SELECT h.store_id, h.category, h.territory_id, h.valid_from,
       h.valid_to_raw AS valid_to_origem, h.valid_to AS valid_to_silver,
       n.territory_id AS territorio_que_substitui, n.assignment_source AS origem_nova
FROM workspace.ms_silver.territorio_historico h
JOIN workspace.ms_silver.territorio_historico n
  ON n.store_id = h.store_id AND n.category = h.category AND n.valid_from = date_add(h.valid_to, 1)
WHERE array_contains(h.dq_flags, 'TER_001')
ORDER BY h.store_id, h.category;

-- 4.5 Produto · categoria corrigida (PRD_005 grafia, PRD_006 vazia, PRD_007 conflito)
SELECT product_id, ean, product_description, subcategory,
       category_raw AS categoria_origem, category AS categoria_silver, dq_flags
FROM workspace.ms_silver.dim_produto
WHERE arrays_overlap(dq_flags, array('PRD_005', 'PRD_006', 'PRD_007'))
ORDER BY subcategory, product_id;

-- ------------------------------------------------------------------ 5. GARANTIAS (todas devem retornar 0)
SELECT 'produto: EAN duplicado' AS garantia,
       COUNT(*) - COUNT(DISTINCT ean) AS violacoes FROM workspace.ms_silver.dim_produto
UNION ALL
SELECT 'loja: store_id duplicado',
       COUNT(*) - COUNT(DISTINCT store_id) FROM workspace.ms_silver.dim_loja
UNION ALL
SELECT 'loja: UF fora do domínio',
       COUNT_IF(state NOT IN ('AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA',
                              'PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO')
                OR state IS NULL) FROM workspace.ms_silver.dim_loja
UNION ALL
SELECT 'loja: CNPJ fora de 14 dígitos',
       COUNT_IF(NOT cnpj RLIKE '^[0-9]{14}$') FROM workspace.ms_silver.dim_loja
UNION ALL
SELECT 'loja: coordenada alterada pelo pipeline',
       COUNT_IF(s.latitude <> CAST(b.latitude AS DOUBLE) OR s.longitude <> CAST(b.longitude AS DOUBLE))
FROM workspace.ms_silver.dim_loja s JOIN workspace.ms_bronze.dim_loja b USING (store_id)
UNION ALL
SELECT 'território: vigência sobreposta', COUNT(*)
FROM (SELECT valid_to, LEAD(valid_from) OVER (PARTITION BY store_id, category ORDER BY valid_from) AS prox
      FROM workspace.ms_silver.territorio_historico)
WHERE prox IS NOT NULL AND valid_to >= prox;

-- ------------------------------------------------------------------ 6. VISÃO DE NEGÓCIO
-- 6.1 Status final por tabela
SELECT 'dim_produto' AS tabela, dq_status, COUNT(*) AS registros FROM workspace.ms_silver.dim_produto GROUP BY ALL
UNION ALL
SELECT 'dim_loja', dq_status, COUNT(*) FROM workspace.ms_silver.dim_loja GROUP BY ALL
UNION ALL
SELECT 'territorio_historico', dq_status, COUNT(*) FROM workspace.ms_silver.territorio_historico GROUP BY ALL
ORDER BY tabela, dq_status;

-- 6.2 Quarentena das dimensões, por regra
SELECT alvo, regra_id, classificacao, COUNT(*) AS registros, MIN(motivo) AS exemplo_motivo
FROM workspace.ms_dq.quarantine
WHERE alvo IN ('dim_produto', 'dim_loja', 'territorio_historico')
GROUP BY ALL
ORDER BY alvo, regra_id;

-- 6.3 Fila de revalidação para o time de cadastro
SELECT * FROM workspace.ms_dq.vw_revalidacao_loja ORDER BY regra_id, store_id;