BEGIN;

DROP INDEX IF EXISTS ix_dedup_mineral_site_country_val_gin;
DROP INDEX IF EXISTS ix_dedup_mineral_site_state_or_province_val_gin;
DROP INDEX IF EXISTS ix_mineral_inventory_view_site_id_commodity;

COMMIT;
