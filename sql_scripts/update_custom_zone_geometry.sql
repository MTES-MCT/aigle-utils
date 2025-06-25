UPDATE
    "public".core_geozone
SET
    geometry = core_geozone_temp.geometry
FROM
    "temp".core_geozone core_geozone_temp
WHERE
    core_geozone_temp."name" = core_geozone."name"
    AND core_geozone.geo_zone_type = 'CUSTOM'
    SELECT
        "name",
        "id"
    FROM
        core_geozone
    WHERE
        "name" ILIKE '%ille%'
        AND core_geozone.geo_zone_type = 'CUSTOM'
