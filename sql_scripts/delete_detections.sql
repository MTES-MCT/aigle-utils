-- description: completely remove detections attached to specific batch from database
DELETE FROM core_detection
WHERE batch_id = 'sia_2021';

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

