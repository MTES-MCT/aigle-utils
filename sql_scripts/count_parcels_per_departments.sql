SELECT
    core_geozonedpt."name",
    core_geodepartment.insee_code,
    count(*)
FROM
    public.core_parcel
    JOIN public.core_geocommune ON core_geocommune.geozone_ptr_id = core_parcel.commune_id
    JOIN public.core_geodepartment ON core_geodepartment.geozone_ptr_id = core_geocommune.department_id
    JOIN public.core_geozone core_geozonedpt ON core_geozonedpt.id = core_geodepartment.geozone_ptr_id
GROUP BY
    core_geozonedpt."name",
    core_geodepartment.insee_code
