-- description: update links between detections and custom zones
INSERT INTO core_detectionobject_geo_custom_zones (detectionobject_id, geocustomzone_id)
SELECT DISTINCT
    dobj.id AS detectionobject_id,
    {custom_zone_id} AS geocustomzone_id
FROM
    core_detectionobject dobj
    JOIN core_detection detec ON detec.detection_object_id = dobj.id
WHERE
    ST_Within (detec.geometry, (
            SELECT
                geozone.geometry
            FROM core_geozone geozone
            WHERE
                id = {custom_zone_id}))
ON CONFLICT
    DO NOTHING;

