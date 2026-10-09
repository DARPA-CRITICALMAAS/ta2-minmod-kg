-- Problem 2 acceptance: merged entities whose state belongs to none of the
-- entity's countries. Read-only. Run against the kgrel Postgres AFTER the
-- merged table is rebuilt with the fix (the merge cache bump ships with the
-- P1+P4 PR).
--
-- SQL, not SPARQL: normalized ids live only in Postgres
-- (dedup_mineral_site.country_val / state_or_province_val, VARCHAR(7)[]), and
-- this fix deliberately leaves the RDF unchanged.
--
-- Before: 5,858 (as reported). A local rebuild of the same data gives 5,856.
-- Expected after: 0 on the pinned data (5,856 -> 0, measured on a local load
-- of the patched ETL output). States the rule cannot repair are dropped, not
-- left in conflict. Live data can differ; query 2 lists whatever remains.

-- 1. The count.
SELECT count(*) AS merged_entities_with_state_in_another_country
FROM dedup_mineral_site AS d
WHERE
    cardinality(d.country_val) > 0
    AND EXISTS (
        SELECT 1
        FROM unnest(d.state_or_province_val) AS s (id)
        INNER JOIN state_or_province AS sp ON s.id = sp.id
        WHERE
            sp.country IS NOT NULL
            AND NOT sp.country = any(d.country_val)
    );

-- 2. The survivors, for triage: the observed name behind each conflicting
-- state (from the record the state was elected from), grouped by the entity's
-- countries.
WITH conflicts AS (
    SELECT
        d.id,
        d.country_val,
        d.state_or_province_refid AS refid,
        s.id AS state_id
    FROM dedup_mineral_site AS d
    CROSS JOIN LATERAL unnest(d.state_or_province_val) AS s (id)
    INNER JOIN state_or_province AS sp ON s.id = sp.id
    WHERE
        cardinality(d.country_val) > 0
        AND sp.country IS NOT NULL
        AND NOT sp.country = any(d.country_val)
)

SELECT
    cand ->> 'observed_name' AS observed_name,
    array_to_string(
        array(
            SELECT c.name FROM country AS c
            WHERE c.id = any(conflicts.country_val)
            ORDER BY c.name
        ),
        ', '
    ) AS countries,
    count(DISTINCT conflicts.id) AS merged_entities
FROM conflicts
INNER JOIN mineral_site AS ms ON conflicts.refid = ms.site_id
CROSS JOIN
    LATERAL jsonb_array_elements(
        convert_from(ms.location, 'UTF8')::jsonb -> 'state_or_province'
    ) AS cand
WHERE
    cand ->> 'normalized_uri'
    = 'https://minmod.isi.edu/resource/' || conflicts.state_id
GROUP BY observed_name, countries
ORDER BY merged_entities DESC, observed_name ASC;
