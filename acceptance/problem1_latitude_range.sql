-- Problem 1 acceptance: merged (dedup) sites whose latitude is outside [-90, 90].
-- Run against the kgrel Postgres AFTER the merged table is rebuilt. Read-only.
--
-- Why SQL and not SPARQL: reprojected lat/lon exist only in Postgres
-- (dedup_mineral_site.coordinates, mineral_site.location_view). The RDF graph stores
-- the raw source WKT + CRS, which this fix does not change, so no SPARQL query can see it.
-- Both columns are bytea holding orjson (DataclassType in minmodkg/models/kgrel/custom_types).
--
-- Before: 24,818 merged sites (the EPSG:2994 Oregon and EPSG:26912 Utah sites);
-- the per-record query below counts 31,109 source records.
-- Expected after: 1 in both (Tagaung Taung, EPSG:4326, swapped in the source
--   data; needs a manual fix in ta2-minmod-data, not code).

SELECT count(*) AS dedup_sites_lat_out_of_range
FROM dedup_mineral_site
WHERE coordinates IS NOT NULL
  AND abs((convert_from(coordinates, 'UTF8')::jsonb -> 'value' ->> 'lat')::float8) > 90;

-- Same check per source record, grouped by CRS, to see where any remainder comes from:
SELECT convert_from(location, 'UTF8')::jsonb -> 'crs' ->> 'observed_name' AS crs,
       count(*) AS sites_lat_out_of_range
FROM mineral_site
WHERE location_view IS NOT NULL
  AND abs((convert_from(location_view, 'UTF8')::jsonb ->> 'lat')::float8) > 90
GROUP BY 1
ORDER BY 2 DESC;
