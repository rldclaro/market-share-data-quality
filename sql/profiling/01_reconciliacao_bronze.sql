-- =============================================================================
-- 01 · Reconciliação da Bronze (regra ING_001)
--
-- Objetivo : conferir se as linhas carregadas batem com o volume declarado no
--            README do pacote de dados, para cada arquivo de origem.
-- Achado   : os 7 arquivos batem (1.235 / 2.500 / 91 / 4.736 / 6.370 / 68.526 / 58.526).
-- Regra    : ING_001 — uma regra, aplicada a 7 alvos.
-- =============================================================================

SELECT 'ING_001'     AS regra_id,
       table_name    AS alvo,
       expected_rows AS esperadas_readme,
       loaded_rows   AS carregadas,
       sha256,
       status
FROM workspace.ms_dq.bronze_load_log
QUALIFY ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY loaded_at DESC) = 1   -- última carga de cada tabela
ORDER BY alvo;