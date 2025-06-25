WITH tileset_geozones AS (
    SELECT
        ts.id AS tileset_id,
        ST_Union (gz.geometry) AS combined_geometry
    FROM
        core_tileset ts
        JOIN core_tileset_geo_zones tsgz ON tsgz.tileset_id = ts.id
        JOIN core_geozone gz ON gz.id = tsgz.geozone_id
    GROUP BY
        ts.id)
DELETE FROM core_detection
WHERE id IN (
        SELECT
            d.id
        FROM
            core_detection d
        LEFT JOIN tileset_geozones tg ON tg.tileset_id = d.tile_set_id
    WHERE
        tg.tileset_id IS NULL
        OR NOT ST_Contains (tg.combined_geometry, d.geometry));

DELETE FROM core_detectiondata
WHERE id IN (
        SELECT
            core_detectiondata.id
        FROM
            core_detectiondata
        LEFT JOIN core_detection ON core_detectiondata.id = core_detection.detection_data_id
    WHERE
        core_detection.detection_data_id IS NULL);

DELETE FROM core_detectionobject_geo_custom_zones
WHERE detectionobject_id IN (
        SELECT
            obj.id
        FROM
            core_detectionobject AS obj
        LEFT JOIN core_detection AS det ON obj.id = det.detection_object_id
    WHERE
        det.detection_object_id IS NULL);

DELETE FROM core_detectionobject
WHERE id IN (
        SELECT
            obj.id
        FROM
            core_detectionobject AS obj
        LEFT JOIN core_detection AS det ON obj.id = det.detection_object_id
    WHERE
        det.detection_object_id IS NULL);

