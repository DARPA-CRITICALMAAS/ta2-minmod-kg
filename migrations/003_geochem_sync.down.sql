BEGIN;

DROP INDEX IF EXISTS ix_event_log_geochem_kg_synced;
DROP INDEX IF EXISTS ix_event_log_geochem_synced;
ALTER TABLE event_log DROP COLUMN IF EXISTS geochem_kg_synced;
ALTER TABLE event_log DROP COLUMN IF EXISTS geochem_synced;

COMMIT;
