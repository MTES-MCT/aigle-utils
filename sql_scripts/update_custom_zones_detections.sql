-- description: update links between detections and custom zones
INSERT INTO core_detectionobject_geo_custom_zones (detectionobject_id, geocustomzone_id)
SELECT DISTINCT
    core_detectionobject.id AS detectionobject_id,
    core_geozone.id AS geocustomzone_id
FROM
    core_detectionobject
    JOIN core_detection ON core_detection.detection_object_id = core_detectionobject.id
    JOIN core_geozone ON ST_Covers (core_geozone.geometry, core_detection.geometry)
ON CONFLICT
    DO NOTHING;

INSERT INTO core_detectionobject_geo_sub_custom_zones (detectionobject_id, geosubcustomzone_id)
SELECT DISTINCT
    core_detectionobject.id AS detectionobject_id,
    core_geosubcustomzone.id AS geosubcustomzone_id
FROM
    core_detectionobject
    JOIN core_detection ON core_detection.detection_object_id = core_detectionobject.id
    JOIN core_geosubcustomzone ON ST_Covers (core_geosubcustomzone.geometry, core_detection.geometry)
ON CONFLICT
    DO NOTHING;

-- old version
INSERT INTO public.core_detectionobject_geo_custom_zones (detectionobject_id, geocustomzone_id)
SELECT DISTINCT
    dobj.id AS detectionobject_id,
    {custom_zone_id} AS geocustomzone_id
FROM
    public.core_detectionobject dobj
    JOIN public.core_detection detec ON detec.detection_object_id = dobj.id
WHERE
    ST_Within (detec.geometry, (
            SELECT
                geozone.geometry
            FROM public.core_geozone geozone
            WHERE
                id = {custom_zone_id})) ON CONFLICT
                DO NOTHING;

