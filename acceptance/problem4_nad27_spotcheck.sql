-- Problem 4 acceptance: post-rebuild lat/lon of 5 NAD27 (EPSG:4267) Alaska sites. Read-only.
-- Expected values = PROJ's NAD27 -> WGS84 result with the NOAA NADCON (us_noaa_alaska)
-- and Canada NTv2 (ca_nrc_ntv2_0) grids, which the backend image ships. Without grids
-- (pyproj default, PROJ_NETWORK off) expect the "no-grid" column; sl004 then does not move.
--
-- site                        why chosen                     nadcon lat/lon               no-grid lat/lon
-- at001 Abraham Bay (Attu)    west of antimeridian, 274.8 m  52.84834456  172.8169732     52.8483413  172.8169921
-- sl004 Booshu Camp (St.L.I.) no non-grid op covers it       63.45706406 -171.8257914     63.458     -171.823  (unmoved)
-- ya005 Sitkagi Bluffs        SE AK: PROJ uses CA NTv2 grid  59.69963092 -140.4720642     59.6995514 -140.4719058
-- dl001 Red Dog               major deposit                  68.06974892 -162.8408531     68.0697859 -162.841029
-- id162 Donlin Bench          major deposit                  62.0490064  -158.2386103     62.0490122 -158.2386321
-- Before the fix all five equal their raw, unconverted input coordinates.

WITH expected(site_id, lat, lon) AS (VALUES
  ('site__doi-org-10-5066-p96mmrfd__at001__umn', 52.84834456,  172.8169732),
  ('site__doi-org-10-5066-p96mmrfd__sl004__umn', 63.45706406, -171.8257914),
  ('site__doi-org-10-5066-p96mmrfd__ya005__umn', 59.69963092, -140.4720642),
  ('site__doi-org-10-5066-p96mmrfd__dl001__umn', 68.06974892, -162.8408531),
  ('site__doi-org-10-5066-p96mmrfd__id162__umn', 62.0490064,  -158.2386103)
)
SELECT e.site_id,
       (convert_from(m.location_view, 'UTF8')::jsonb ->> 'lat')::float8 AS lat,
       (convert_from(m.location_view, 'UTF8')::jsonb ->> 'lon')::float8 AS lon,
       e.lat AS expected_lat, e.lon AS expected_lon,
       -- rough metres; < 1 means grid-accurate, ~2-25 means no-grid Helmert, >100 means unfixed
       round((111320 * sqrt(power((convert_from(m.location_view, 'UTF8')::jsonb ->> 'lat')::float8 - e.lat, 2)
             + power(((convert_from(m.location_view, 'UTF8')::jsonb ->> 'lon')::float8 - e.lon) * cos(radians(e.lat)), 2)))::numeric, 1) AS approx_err_m
FROM expected e LEFT JOIN mineral_site m ON m.site_id = e.site_id
ORDER BY e.site_id;
