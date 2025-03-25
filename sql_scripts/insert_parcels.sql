-- prerequisites: run scripts to insert data into parcels.{dpt_insee}

insert into core_parcel(
    arpente,
    commune_id,
    contenance,
    created_at,
    deleted,
    deleted_at,
    geometry,
    id_parcellaire,
    num_parcel,
    prefix,
    refreshed_at,
    section,
    updated_at,
    uuid
)
select
    false as arpente,
    core_geocommune.geozone_ptr_id as commune_id,
    coalesce({dpt_insee}.contenance, 0) as contenance,
    now() as created_at,
    false as deleted,
    null as deleted_at,
    {dpt_insee}.geometry as geometry,
    {dpt_insee}.idu as id_parcellaire,
    TRIM(LEADING '0' FROM {dpt_insee}.numero) as num_parcel,
    {dpt_insee}.com_abs as prefix,
    MAKE_DATE(2025,1,1) as refreshed_at, -- get that date from ign source file
    {dpt_insee}.section as section,
    now() as updated_at,
    gen_random_uuid() as uuid
from
    parcels.{dpt_insee}
join
    "public".core_geocommune on core_geocommune.iso_code = {dpt_insee}.code_dep || {dpt_insee}.code_com
