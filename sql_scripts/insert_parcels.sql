-- prerequisites: run scripts to insert data into parcels.{dpt_insee}
INSERT INTO core_parcel (arpente, commune_id, contenance, created_at, deleted, deleted_at, geometry, id_parcellaire, num_parcel, prefix, refreshed_at, section, updated_at, uuid)
SELECT
    FALSE AS arpente,
    core_geocommune.geozone_ptr_id AS commune_id,
    coalesce({dpt_insee}.contenance, 0) AS contenance,
    now() AS created_at,
    FALSE AS deleted,
    NULL AS deleted_at,
    {dpt_insee}.geometry AS geometry,
    {dpt_insee}.idu AS id_parcellaire,
    TRIM(LEADING '0' FROM {dpt_insee}.numero) AS num_parcel, {dpt_insee}.com_abs AS prefix, MAKE_DATE(2025, 1, 1) AS refreshed_at, -- get that date from ign source file
        {dpt_insee}.section AS section, now() AS updated_at, gen_random_uuid () AS uuid FROM parcels. {dpt_insee}
JOIN "public".core_geocommune ON core_geocommune.iso_code = {dpt_insee}.code_dep || {dpt_insee}.code_com
