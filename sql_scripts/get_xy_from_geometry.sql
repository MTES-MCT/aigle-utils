-- description: extract xy coordinates from a geometry in database (here, a geozone), useful to run command create_tile
WITH bbox AS (
    SELECT
        ST_XMin (ST_Envelope (geometry)) AS min_lon,
        ST_YMin (ST_Envelope (geometry)) AS min_lat,
        ST_XMax (ST_Envelope (geometry)) AS max_lon,
        ST_YMax (ST_Envelope (geometry)) AS max_lat
    FROM
        core_geozone
    WHERE
        uuid = {geozone_uuid}
)
SELECT
    FLOOR((min_lon + 180) / 360 * POW(2, 19)) AS min_x_tile,
    FLOOR((1 - LN(TAN(RADIANS(max_lat)) + 1 / COS(RADIANS(max_lat))) / PI()) / 2 * POW(2, 19)) AS min_y_tile,
    FLOOR((max_lon + 180) / 360 * POW(2, 19)) AS max_x_tile,
    FLOOR((1 - LN(TAN(RADIANS(min_lat)) + 1 / COS(RADIANS(min_lat))) / PI()) / 2 * POW(2, 19)) AS max_y_tile
FROM
    bbox;

