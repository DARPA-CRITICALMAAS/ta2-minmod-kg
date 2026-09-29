-- Who registered a GeoChem paper through POST /papers. Idempotent.
BEGIN;

ALTER TABLE paper ADD COLUMN IF NOT EXISTS registered_by VARCHAR;

COMMIT;
