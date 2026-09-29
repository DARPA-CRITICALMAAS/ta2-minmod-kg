-- Indexes for searching papers and dedup sites. Idempotent.
BEGIN;

-- GIN, so country/state containment filters (@>) use an index
CREATE INDEX IF NOT EXISTS ix_dedup_mineral_site_country_val_gin
ON dedup_mineral_site USING gin (country_val);
CREATE INDEX IF NOT EXISTS ix_dedup_mineral_site_state_or_province_val_gin
ON dedup_mineral_site USING gin (state_or_province_val);

-- "does this site have this commodity"
CREATE INDEX IF NOT EXISTS ix_mineral_inventory_view_site_id_commodity
ON mineral_inventory_view (site_id, commodity);

COMMIT;
