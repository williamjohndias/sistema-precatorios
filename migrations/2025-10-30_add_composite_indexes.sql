-- Composite and selective indexes to speed up main listing and dropdowns
-- Safe to run multiple times due to IF NOT EXISTS

BEGIN;

-- Main list: default filter and sort
CREATE INDEX IF NOT EXISTS idx_precatorios_esta_ordem_ordem
    ON precatorios(esta_na_ordem, ordem);

-- Range filter on valor combined with default filter
CREATE INDEX IF NOT EXISTS idx_precatorios_esta_ordem_valor
    ON precatorios(esta_na_ordem, valor);

-- Dropdown helpers (optional but recommended)
CREATE INDEX IF NOT EXISTS idx_precatorios_esta_ordem_prioridade
    ON precatorios(esta_na_ordem, prioridade);

CREATE INDEX IF NOT EXISTS idx_precatorios_esta_ordem_regime
    ON precatorios(esta_na_ordem, regime);

CREATE INDEX IF NOT EXISTS idx_precatorios_esta_ordem_tribunal
    ON precatorios(esta_na_ordem, tribunal);

CREATE INDEX IF NOT EXISTS idx_precatorios_esta_ordem_natureza
    ON precatorios(esta_na_ordem, natureza);

-- Analyze to refresh planner stats
ANALYZE precatorios;

COMMIT;


