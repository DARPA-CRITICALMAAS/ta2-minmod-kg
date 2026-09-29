-- GeoChem write-back gets its own event flag, so it runs apart from
-- MinMod's backup. Idempotent.
BEGIN;

ALTER TABLE event_log
ADD COLUMN IF NOT EXISTS geochem_synced BOOLEAN NOT NULL DEFAULT true;
CREATE INDEX IF NOT EXISTS ix_event_log_geochem_synced
ON event_log (geochem_synced);

COMMIT;
