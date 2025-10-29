-- Migration suggestion: standardize column types for better sorting and validation
-- Review on a staging copy before applying in production.

BEGIN;

-- Convert ordem to integer (from text) preserving numeric order
ALTER TABLE precatorios
  ALTER COLUMN ordem TYPE integer
  USING NULLIF(regexp_replace(ordem, '[^0-9]', '', 'g'), '')::integer;

-- Convert ano_orc to integer if it is text
-- If already integer, this statement will need adjustment or can be skipped
ALTER TABLE precatorios
  ALTER COLUMN ano_orc TYPE integer
  USING NULLIF(regexp_replace(ano_orc, '[^0-9]', '', 'g'), '')::integer;

-- Convert valor to numeric(14,2). Adjust precision/scale if needed
ALTER TABLE precatorios
  ALTER COLUMN valor TYPE numeric(14,2)
  USING (
    CASE
      WHEN valor IS NULL THEN NULL
      ELSE (
        -- Remove currency symbols/spaces, normalize comma to dot
        CASE WHEN regexp_replace(valor, '\\s', '', 'g') = '' THEN NULL
             ELSE replace(regexp_replace(valor, '[^0-9.,]', '', 'g'), ',', '.')::numeric
        END
      )
    END
  );

-- Helpful indexes for ordering and filtering (IMPORTANT FOR PERFORMANCE)
CREATE INDEX IF NOT EXISTS idx_precatorios_ordem ON precatorios(ordem);
CREATE INDEX IF NOT EXISTS idx_precatorios_esta_na_ordem ON precatorios(esta_na_ordem) WHERE esta_na_ordem = TRUE;
CREATE INDEX IF NOT EXISTS idx_precatorios_organizacao ON precatorios(organizacao) WHERE organizacao IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_precatorios_prioridade ON precatorios(prioridade) WHERE prioridade IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_precatorios_tribunal ON precatorios(tribunal) WHERE tribunal IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_precatorios_ano_orc ON precatorios(ano_orc) WHERE ano_orc IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_precatorios_valor ON precatorios(valor) WHERE valor IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_precatorios_situacao ON precatorios(situacao) WHERE situacao IS NOT NULL;

COMMIT;

