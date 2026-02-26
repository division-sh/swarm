-- Consolidate duplicate spend metadata fields.
UPDATE spend_ledger
SET metadata = COALESCE(metadata, meta)
WHERE metadata IS NULL
  AND meta IS NOT NULL;

ALTER TABLE spend_ledger
  DROP COLUMN IF EXISTS meta;
