-- description: update links between parcels and custom zones
DO $$
DECLARE
    zone_record RECORD;
    inserted_count integer;
BEGIN
    -- Loop through each geozone
    FOR zone_record IN
    SELECT
        id,
        geometry
    FROM
        core_geozone
    ORDER BY
        id LOOP
            -- Insert parcels for current zone
            INSERT INTO core_parcel_geo_custom_zones (parcel_id, geocustomzone_id)
            SELECT
                cp.id AS parcel_id,
                zone_record.id AS geocustomzone_id
            FROM
                core_parcel cp
            WHERE
                ST_Intersects (cp.geometry, zone_record.geometry)
            ON CONFLICT
                DO NOTHING;
            -- Get count of inserted rows
            GET DIAGNOSTICS inserted_count = ROW_COUNT;
            -- Commit the transaction for this zone
            COMMIT;
            -- Optional: Log progress
            RAISE NOTICE 'Processed zone_id: %, inserted: % parcels', zone_record.id, inserted_count;
        END LOOP;
END
$$;

INSERT INTO core_parcel_geo_custom_zones (parcel_id, geocustomzone_id)
SELECT DISTINCT
    core_parcel.id AS parcel_id,
    {custom_zone_id} AS geocustomzone_id
FROM
    core_parcel
WHERE
    ST_Intersects (core_parcel.geometry, (
            SELECT
                geozone.geometry
            FROM core_geozone geozone
            WHERE
                id = {custom_zone_id}))
ON CONFLICT
    DO NOTHING;

