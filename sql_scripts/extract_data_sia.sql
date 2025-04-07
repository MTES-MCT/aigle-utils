-- description: extract data from the SIA database
SELECT
    rel.id,
    rel.polygon AS "geometry",
    CASE WHEN (rel.dessine_interface) THEN
        1
    WHEN score IS NULL THEN
        1
    ELSE
        rel.score
    END AS score,
    NULL AS "address",
    ann_t.name_n AS "object_type",
    CASE WHEN (rel.dessine_interface) THEN
        'INTERFACE_DRAWN'
    ELSE
        'ANALYSIS'
    END AS "detection_source",
    CASE WHEN rel.signale_terrain THEN
        'CONTROLLED_FIELD'
    WHEN rel.control_status_id = 1 THEN
        'NOT_CONTROLLED'
    WHEN rel.control_status_id = 2 THEN
        'SIGNALED_COMMUNE'
    WHEN rel.control_status_id = 3 THEN
        'SIGNALED_COLLECTIVITY'
    WHEN rel.control_status_id = 4 THEN
        'CONTROLLED_FIELD'
    WHEN rel.control_status_id = 5 THEN
        'REHABILITATED'
    WHEN rel.control_status_id = 6 THEN
        'VERBALIZED'
    END AS "detection_control_status",
    CASE WHEN rel.validation = 0 THEN
        'INVALIDATED'
    WHEN rel.vrai_legitime
        AND rel.vrai_positif
        AND NOT rel.faux_positif THEN
        'LEGITIMATE'
    WHEN NOT rel.vrai_legitime
        AND rel.vrai_positif
        AND NOT rel.faux_positif THEN
        'SUSPECT'
    WHEN NOT rel.vrai_legitime
        AND NOT rel.vrai_positif
        AND rel.faux_positif THEN
        'INVALIDATED'
    WHEN NOT rel.vrai_legitime
        AND NOT rel.vrai_positif
        AND NOT rel.faux_positif THEN
        'DETECTED_NOT_VERIFIED'
    ELSE
        'DETECTED_NOT_VERIFIED'
    END AS "detection_validation_status",
    CASE WHEN rel.prescrit_manuel THEN
        'PRESCRIBED'
    ELSE
        'NOT_PRESCRIBED'
    END AS "detection_prescription_status",
    rel.validation IS NOT NULL AS "user_reviewed",
    NULL AS tile_x,
    NULL AS tile_y,
    CASE WHEN tiles.dataset_id = 7 THEN
        'sia_2012'
    WHEN tiles.dataset_id = 4 THEN
        'sia_2015'
    WHEN tiles.dataset_id = 5 THEN
        'sia_2018'
    WHEN tiles.dataset_id = 8 THEN
        'sia_2021'
    END AS "batch_id"
FROM
    relevant_detections rel
    JOIN annotation_types ann_t ON (
        CASE WHEN rel.validation IS NULL
            OR rel.validation = 0 THEN
            rel.type_id
        ELSE
            rel.validation
        END) = ann_t.id
JOIN tiles ON tiles.id = rel.tile_id
WHERE
    tiles.dataset_id IN (4, 5, 7, 8)
ORDER BY
    score DESC
