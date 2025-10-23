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

-- Helpful index for ordering by ordem
CREATE INDEX IF NOT EXISTS idx_precatorios_ordem ON precatorios(ordem);

COMMIT;


