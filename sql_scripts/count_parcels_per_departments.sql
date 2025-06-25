SELECT
    core_geozonedpt."name",
    core_geodepartment.insee_code,
    count(*)
FROM
    core_parcel
    JOIN core_geocommune ON core_geocommune.geozone_ptr_id = core_parcel.commune_id
    JOIN core_geodepartment ON core_geodepartment.geozone_ptr_id = core_geocommune.department_id
    JOIN core_geozone core_geozonedpt ON core_geozonedpt.id = core_geodepartment.geozone_ptr_id
GROUP BY
    core_geozonedpt."name",
    core_geodepartment.insee_code
