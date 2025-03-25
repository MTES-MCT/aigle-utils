-- prerequisites: run scripts communes.sql, departments.sql, regions.sql

CREATE
OR REPLACE FUNCTION get_surface_km2(geom GEOMETRY) RETURNS double precision AS $$ BEGIN RETURN ST_Area(ST_Transform(geom, 3857)) / 1e6;

END;

$$ LANGUAGE plpgsql;

CREATE
OR REPLACE FUNCTION normalize_text(input_text TEXT) RETURNS TEXT AS $$ DECLARE res TEXT;

BEGIN res := unaccent(input_text);

res := regexp_replace(res, '[^a-zA-Z0-9 \n\.]', ' ', 'g');

res := lower(res);

res := regexp_replace(res, '\s+', ' ', 'g');

RETURN res;

END;

$$ LANGUAGE plpgsql;

----
UPDATE
    "public".core_geozone
SET
    geometry = (
        SELECT
            geometry
        FROM
            regions
        WHERE
            insee_reg = (
                SELECT
                    insee_code
                FROM
                    core_georegion
                WHERE
                    core_georegion.geozone_ptr_id = core_geozone.id
            )
    )
WHERE
    id IN (
        SELECT
            id
        FROM
            core_geozone
            JOIN core_georegion ON core_georegion.geozone_ptr_id = core_geozone.id
        WHERE
            core_georegion.insee_code IN (
                SELECT
                    DISTINCT insee_reg
                FROM
                    regions
            )
    );

INSERT INTO
    "public".core_geozone(
        uuid,
        created_at,
        updated_at,
        deleted,
        geo_zone_type,
        geometry,
        "name",
        name_normalized
    )
SELECT
    gen_random_uuid() AS "uuid",
    now() AS created_at,
    now() AS updated_at,
    FALSE AS "deleted",
    'REGION' as "geo_zone_type",
    geometry,
    nom AS "name",
    normalize_text(nom) AS name_normalized
FROM
    "public".regions
WHERE
    insee_reg NOT IN (
        SELECT
            insee_code
        FROM
            core_georegion
    );

INSERT INTO
    "public".core_georegion(
        geozone_ptr_id,
        insee_code,
        surface_km2
    )
SELECT
    core_geozone.id AS geozone_ptr_id,
    regions.insee_reg AS insee_code,
    get_surface_km2(core_geozone.geometry) AS surface_km2
FROM
    "public".core_geozone
    LEFT JOIN "public".core_georegion ON core_georegion.geozone_ptr_id = core_geozone.id
    JOIN "public".regions ON regions."nom" = core_geozone."name"
WHERE
    true
    AND geo_zone_type = 'REGION'
    AND core_georegion.geozone_ptr_id IS NULL;

--
UPDATE
    "public".core_geozone
SET
    geometry = (
        SELECT
            geometry
        FROM
            departments
        WHERE
            insee_dep = (
                SELECT
                    insee_code
                FROM
                    core_geodepartment
                WHERE
                    core_geodepartment.geozone_ptr_id = core_geozone.id
            )
    )
WHERE
    id IN (
        SELECT
            id
        FROM
            core_geozone
            JOIN core_geodepartment ON core_geodepartment.geozone_ptr_id = core_geozone.id
        WHERE
            core_geodepartment.insee_code IN (
                SELECT
                    DISTINCT insee_dep
                FROM
                    departments
            )
    );

INSERT INTO
    "public".core_geozone(
        uuid,
        created_at,
        updated_at,
        deleted,
        geo_zone_type,
        geometry,
        "name",
        name_normalized
    )
SELECT
    gen_random_uuid() AS "uuid",
    now() AS created_at,
    now() AS updated_at,
    FALSE AS "deleted",
    'DEPARTMENT' as "geo_zone_type",
    geometry,
    nom AS "name",
    normalize_text(nom) AS name_normalized
FROM
    "public".departments
WHERE
    insee_dep NOT IN (
        SELECT
            insee_code
        FROM
            core_geodepartment
    );

INSERT INTO
    "public".core_geodepartment(
        geozone_ptr_id,
        insee_code,
        region_id,
        surface_km2
    )
SELECT
    core_geozone.id AS geozone_ptr_id,
    departments.insee_dep AS insee_code,
    (
        SELECT
            geozone_ptr_id
        FROM
            core_georegion
        WHERE
            insee_code = departments.insee_reg
    ) AS region_id,
    get_surface_km2(core_geozone.geometry) AS surface_km2
FROM
    "public".core_geozone
    LEFT JOIN "public".core_geodepartment ON core_geodepartment.geozone_ptr_id = core_geozone.id
    JOIN "public".departments ON departments."nom" = core_geozone."name"
WHERE
    true
    AND geo_zone_type = 'DEPARTMENT'
    AND core_geodepartment.geozone_ptr_id IS NULL;

--
UPDATE
    "public".core_geozone
SET
    geometry = (
        SELECT
            geometry
        FROM
            "public".communes
        WHERE
            insee_com = (
                SELECT
                    iso_code
                FROM
                    core_geocommune
                WHERE
                    core_geocommune.geozone_ptr_id = core_geozone.id
            )
    )
WHERE
    id IN (
        SELECT
            id
        FROM
            "public".core_geozone
            JOIN "public".core_geocommune ON core_geocommune.geozone_ptr_id = core_geozone.id
        WHERE
            core_geocommune.iso_code IN (
                SELECT
                    DISTINCT insee_com
                FROM
                    communes
            )
    );

INSERT INTO
    "public".core_geozone(
        uuid,
        created_at,
        updated_at,
        deleted,
        geo_zone_type,
        geometry,
        "name",
        name_normalized
    )
SELECT
    gen_random_uuid() AS "uuid",
    now() AS created_at,
    now() AS updated_at,
    FALSE AS "deleted",
    'COMMUNE' as "geo_zone_type",
    geometry,
    nom AS "name",
    normalize_text(nom) AS name_normalized
FROM
    "public".communes
WHERE
    insee_com NOT IN (
        SELECT
            iso_code
        FROM
            core_geocommune
    );

INSERT INTO
    "public".core_geocommune(
        department_id,
        geozone_ptr_id,
        iso_code
    )
SELECT
    (
        SELECT
            geozone_ptr_id
        FROM
            core_geodepartment
        WHERE
            insee_code = communes.insee_dep
    ) AS department_id,
    core_geozone.id AS geozone_ptr_id,
    communes.insee_com AS iso_code
FROM
    "public".core_geozone
    LEFT JOIN "public".core_geocommune ON core_geocommune.geozone_ptr_id = core_geozone.id
    JOIN "public".communes ON communes."geometry" = core_geozone."geometry"
WHERE
    true
    AND geo_zone_type = 'COMMUNE'
    AND core_geocommune.geozone_ptr_id IS NULL;