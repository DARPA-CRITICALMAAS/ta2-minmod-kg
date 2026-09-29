-- The GeoChem sync gets its own event flags (triple store, JSON-LD
-- write-back), so it runs apart from MinMod's sync. Idempotent.
BEGIN;

ALTER TABLE event_log
ADD COLUMN IF NOT EXISTS geochem_kg_synced BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE event_log
ADD COLUMN IF NOT EXISTS geochem_synced BOOLEAN NOT NULL DEFAULT true;
CREATE INDEX IF NOT EXISTS ix_event_log_geochem_kg_synced
ON event_log (geochem_kg_synced);
CREATE INDEX IF NOT EXISTS ix_event_log_geochem_synced
ON event_log (geochem_synced);

COMMIT;
