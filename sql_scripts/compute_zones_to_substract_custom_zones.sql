--
-- bash/shp_to_postgis.sh {path_to_ZONE_CONSTRUITE.gpkg} zones_construites zones_construites_{department_code}
WITH base_query AS (
    SELECT
        core_geozone."name",
        ST_Intersection (core_geozone.geometry, (
                SELECT
                    geometry
                FROM zones_construites.zones_construites_ {department_code} LIMIT 1)) AS geometry
    FROM
        public.core_geozone
        JOIN public.core_geocommune ON core_geocommune.geozone_ptr_id = core_geozone.id
    WHERE
        TRUE
        AND core_geocommune.iso_code NOT IN ( SELECT DISTINCT
            LEFT (zones_urbaines_ {department_code}.idurba,
                5)
        FROM
            zones_urbaines.zones_urbaines_ {department_code}
        WHERE
            TRUE
            AND zones_urbaines_ {department_code}.typezone IN ('AUs', 'AUc', 'U', 'ZC')
            AND zones_urbaines_ {department_code}.idurba IS NOT NULL)
        AND
        LEFT (core_geocommune.iso_code,
            2) = '{department_code}'
),
buffered_polygons AS (
    SELECT
        name,
        geometry AS original_geometry,
        -- Buffer each individual polygon by 50 meters
        ST_Collect (ARRAY (
                SELECT
                    ST_Transform (ST_Buffer (ST_Transform (geom, 2154), 50), 4326)
                FROM (
                    SELECT
                        (ST_Dump (geometry)).geom) AS dumped_geoms (geom))) AS buffered_geometry
    FROM
        base_query
),
with_max_area AS (
    SELECT
        name,
        original_geometry,
        buffered_geometry,
        -- Calculate max area from the buffered polygons
        (
            SELECT
                MAX(ST_Area (ST_Transform (geom, 2154)))
            FROM (
                SELECT
                    (ST_Dump (buffered_geometry)).geom) AS dumped_geoms (geom)) AS max_area
    FROM
        buffered_polygons
),
result_geometry_exploded AS (
    SELECT
        name,
        original_geometry AS geometry,
        max_area,
        -- Filter buffered polygons based on max_area * 0.05
        ST_Buffer (ST_Collect (ARRAY (
                    SELECT
                        geom
                    FROM (
                        SELECT
                            (ST_Dump (buffered_geometry)).geom) AS dumped_geoms (geom)
                    WHERE
                        ST_Area (ST_Transform (geom, 2154)) > max_area * 0.05)), 0) AS geometry_filtered
    FROM
        with_max_area)
INSERT INTO custom_zones.to_deduct_ {department_code}(geometry)
SELECT
    ST_Union ("geometry_filtered")
FROM
    result_geometry_exploded;

