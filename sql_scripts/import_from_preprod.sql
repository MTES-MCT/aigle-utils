-- prerequisites: create temporary table in target database, here for the example: detections.import_from_preprod
WITH geometry_union AS (
    SELECT
        ST_Union (core_geozone.geometry) AS "geometry"
    FROM
        core_geocommune
        JOIN core_geozone ON core_geozone.id = core_geocommune.geozone_ptr_id
    WHERE
        core_geozone."name" IN {collectivities}
)
SELECT
    core_detectionobject.address,
    {unique_batch_identifier} AS batch_id,
    core_detection.created_at,
    core_detectiondata.detection_control_status,
    core_detectiondata.detection_prescription_status,
    core_detection.detection_source,
    core_detectiondata.detection_validation_status,
    core_detection.geometry,
    core_detection.id,
    normalize_text (core_objecttype."name") AS object_type,
    core_detection.score,
    core_tile.x AS tile_x,
    core_tile.y AS tile_y,
    core_detection.updated_at,
    TRUE AS user_reviewed
FROM
    core_detection
    JOIN core_detectiondata ON core_detectiondata.id = core_detection.detection_data_id
    JOIN core_detectionobject ON core_detectionobject.id = core_detection.detection_object_id
    JOIN core_objecttype ON core_objecttype.id = core_detectionobject.object_type_id
    JOIN core_user ON core_user.id = core_detectiondata.user_last_update_id
    JOIN core_tile ON core_tile.id = core_detection.tile_id
WHERE
    TRUE
    AND ST_Intersects (core_detection.geometry, (
            SELECT
                "geometry"
            FROM geometry_union))
    AND core_user."email" != 'user.reviewer.default.aigle@aigle.beta.gouv.fr'
