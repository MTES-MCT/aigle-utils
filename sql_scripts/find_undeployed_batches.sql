-- Collectivities deployed on only part of their imagery years.
--
-- A deploy creates one TileSet per detections.batch (one batch = one imagery year)
-- and queues one import_detections per batch. When the import never ran (or failed),
-- the TileSet sits there DEACTIVATED with last_import_started_at NULL and zero
-- detections, while the data is still available in the `detections` schema.
--
-- Run on PROD.
--
-- Notes:
--  * detections.run.geozone_id is an env-specific FK into core_geozone.id. A NULL
--    geozone_name means the id does not resolve here (schema copied from preprod
--    without remapping -- see aigle-utils/bash/copy_detections_schema.sh).
--  * TileSet <-> batch is matched on the deployed url (batch_tiles_url_to_xyz in
--    core/services/data_deployment.py), falling back to the deployed name
--    "{geozone} ({code}) {year}".
--  * core_detection.batch_id is a varchar holding detections.batch.id.
WITH batches AS (
    SELECT
        b.id AS batch_id,
        b.batch_name,
        b.created_at AS batch_created_at,
        b.batch_tiles_url,
        r.geozone_id,
        r.src_image_year AS year,
        -- mirrors batch_tiles_url_to_xyz()
        'https://tiles.aigle.beta.gouv.fr/' || btrim(regexp_replace(b.batch_tiles_url, '^s3://aigle-tiles/', ''), '/') || '/{z}/{x}/{y}.webp' AS expected_tile_set_url
    FROM
        detections.batch b
        JOIN detections.run r ON r.id = b.run_id
    WHERE
        r.geozone_id IS NOT NULL
),
geozones AS (
    SELECT
        gz.id,
        gz.name,
        gz.geo_zone_type,
        -- the code the deploy uses to disambiguate same-named zones
        COALESCE(dept.insee_code, com.iso_code, epci.siren_code) AS code
    FROM
        core_geozone gz
        LEFT JOIN core_geodepartment dept ON dept.geozone_ptr_id = gz.id
        LEFT JOIN core_geocommune com ON com.geozone_ptr_id = gz.id
        LEFT JOIN core_geoepci epci ON epci.geozone_ptr_id = gz.id
),
-- One pass over detections.inference; batch_id cast to text so this works whether
-- the ML schema types it as bigint or varchar. Drop this CTE (and its columns) if
-- the table is too large to scan.
inference_counts AS (
    SELECT
        batch_id::text AS batch_id,
        count(*) AS n
    FROM
        detections.inference
    GROUP BY
        1
),
status AS (
    SELECT
        b.geozone_id,
        g.name AS geozone_name,
        g.geo_zone_type,
        g.code AS geozone_code,
        b.year,
        b.batch_id,
        b.batch_name,
        b.batch_created_at,
        b.batch_tiles_url,
        COALESCE(inf.n, 0) AS inferences_available,
        ts.id AS tile_set_id,
        ts.name AS tile_set_name,
        ts.tile_set_status,
        ts.last_import_started_at,
        ts.last_import_ended_at,
        det.n AS detections_imported,
        ts_det.n AS detections_on_tile_set,
        CASE WHEN b.batch_tiles_url IS NULL THEN
            'BATCH_NOT_DEPLOYABLE_NO_TILES_URL'
        WHEN b.year IS NULL THEN
            'BATCH_NOT_DEPLOYABLE_NO_IMAGERY_YEAR'
        WHEN ts.id IS NULL THEN
            'NO_TILE_SET_CREATED'
        WHEN ts.last_import_started_at IS NULL THEN
            'TILE_SET_CREATED_IMPORT_NEVER_RAN'
        ELSE
            'IMPORT_STARTED_BUT_NO_DETECTIONS'
        END AS diagnosis
    FROM
        batches b
        LEFT JOIN geozones g ON g.id = b.geozone_id
        LEFT JOIN inference_counts inf ON inf.batch_id = b.batch_id::text
        -- LATERAL + LIMIT 1: url wins over name, and a zone matching both never doubles the row
        LEFT JOIN LATERAL (
            SELECT
                t.*
            FROM
                core_tileset t
            WHERE
                t.url = b.expected_tile_set_url
                OR t.name = g.name || ' (' || g.code || ') ' || b.year::text
            ORDER BY
                (t.url = b.expected_tile_set_url) DESC,
            t.id
        LIMIT 1) ts ON TRUE
    CROSS JOIN LATERAL (
        SELECT
            count(*) AS n
        FROM
            core_detection d
        WHERE
            d.batch_id = b.batch_id::text) det
        LEFT JOIN LATERAL (
            SELECT
                count(*) AS n
            FROM
                core_detection d
            WHERE
                d.tile_set_id = ts.id) ts_det ON ts.id IS NOT NULL
),
-- Window in its own level: a WHERE at the same level would run before it and make
-- the per-geozone totals count only the missing years.
scored AS (
    SELECT
        s.*,
        count(*) OVER (PARTITION BY geozone_id) AS years_total,
    count(*) FILTER (WHERE detections_imported > 0) OVER (PARTITION BY geozone_id) AS years_deployed,
    count(*) FILTER (WHERE detections_imported = 0) OVER (PARTITION BY geozone_id) AS years_missing
FROM
    status s
)
SELECT
    *
FROM
    scored
WHERE
    detections_imported = 0
    -- Partially deployed collectivities only. Comment out to also list the ones
    -- that were never deployed at all.
    AND years_deployed > 0
ORDER BY
    geozone_name NULLS LAST,
    year;

-- ---------------------------------------------------------------------------
-- Summary: one row per collectivity, deployed years vs missing years.
-- ---------------------------------------------------------------------------
WITH batches AS (
    SELECT
        b.id AS batch_id,
        r.geozone_id,
        r.src_image_year AS year
    FROM
        detections.batch b
        JOIN detections.run r ON r.id = b.run_id
    WHERE
        r.geozone_id IS NOT NULL
),
status AS (
    SELECT
        b.geozone_id,
        b.year,
        (
            SELECT
                count(*)
            FROM
                core_detection d
            WHERE
                d.batch_id = b.batch_id::text) AS detections_imported
        FROM
            batches b
)
SELECT
    s.geozone_id,
    gz.name AS geozone_name,
    gz.geo_zone_type,
    count(*) AS years_total,
    count(*) FILTER (WHERE detections_imported > 0) AS years_deployed,
    count(*) FILTER (WHERE detections_imported = 0) AS years_missing,
    string_agg(DISTINCT s.year::text, ', ') FILTER (WHERE detections_imported > 0) AS deployed_years,
    string_agg(DISTINCT s.year::text, ', ') FILTER (WHERE detections_imported = 0) AS missing_years
FROM
    status s
    LEFT JOIN core_geozone gz ON gz.id = s.geozone_id
GROUP BY
    s.geozone_id,
    gz.name,
    gz.geo_zone_type
HAVING
    count(*) FILTER (WHERE detections_imported = 0) > 0
    AND count(*) FILTER (WHERE detections_imported > 0) > 0
ORDER BY
    years_missing DESC,
    geozone_name;

