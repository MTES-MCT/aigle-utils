

-- Step 1: Créer la table d analyse
CREATE TABLE IF NOT EXISTS detections.tmp_detections_analysis (
    id                 BIGINT PRIMARY KEY,
    score              NUMERIC,
    object_type        TEXT,
    geometry           geometry(MultiPolygon, 4326),
    geocustomzone_id   NUMERIC,
    batch_year   NUMERIC,
    id_parcellaire     TEXT,
    first_detection    BOOLEAN
);


-- Step 2: Inserer les données calculées et agrégées
WITH params AS (
    SELECT {geocustomzone_id}::numeric AS commune_geozone_id, {current_commune_batch_id}::numeric AS commune_batch_id
),
batch_data_to_insert AS 
(
SELECT DISTINCT ON (inf.id)
    inf.id,
    inf.score,
    CASE 
        WHEN inf.object_type IN ('construction en dur', 'construction legere yourte etc.', 'container', 'installation legere') THEN 'construction en dur'
        ELSE inf.object_type
    END AS object_type,
    inf.geometry,
    p.commune_geozone_id AS geocustomzone_id,
    p.commune_batch_id AS commune_batch_id,
    CASE 
        WHEN commune_batch_id = {2018_commune_batch_id} THEN 2018
        WHEN commune_batch_id = {2021_commune_batch_id} THEN 2021
        when commune_batch_id = {2024_commune_batch_id} THEN 2024
        ELSE null
    END as batch_year,
    cp.id_parcellaire
FROM
    detections.inference inf 
    JOIN params p ON inf.batch_id = p.commune_batch_id
    LEFT JOIN core_parcel cp 
        ON ST_Intersects(cp.geometry, inf.geometry)
WHERE
    inf.score > 0.3
    and
    ST_Within(ST_SetSRID(inf.geometry, 4326), (
        SELECT geozone.geometry
        FROM core_geozone geozone
        WHERE geozone.id = p.commune_geozone_id
    ))
    AND inf.object_type IN (
        'construction en dur',
        'piscine',
        'mobil home',
        'caravane',
        'construction legere yourte etc.',
        'container',
        'installation legere'
    )
    and 
   (
   ST_Within(ST_SetSRID(inf.geometry, 4326), (
        SELECT geozone.geometry
        FROM core_geozone geozone
        WHERE geozone.id = 361 -- zone enjeu env herault
    ))
   or 
   ST_Within(ST_SetSRID(inf.geometry, 4326), (
        SELECT geozone.geometry
        FROM core_geozone geozone
        WHERE geozone.id = 352 -- zone risque fort herault
    ))
    or 
   ST_Within(ST_SetSRID(inf.geometry, 4326), (
        SELECT geozone.geometry
        FROM core_geozone geozone
        WHERE geozone.id = 351 -- zone naturelle et agricole herault
    ))
   )
   ORDER BY inf.id, cp.id  -- garder seulement la premiere parcelle associée a chaque detection
)
INSERT INTO detections.tmp_detections_analysis (
    id,
    score,
    object_type,
    geometry,
    geocustomzone_id,
    batch_year,
    id_parcellaire,
    first_detection
)
SELECT 
    t.id,
    t.score,
    t.object_type,
    t.geometry,
    t.geocustomzone_id,
    t.batch_year,
    t.id_parcellaire,
    NOT EXISTS (
        SELECT 1
        FROM detections.tmp_detections_analysis existing
        WHERE 
            ST_Intersects(t.geometry, existing.geometry)
            AND ST_Area(ST_Intersection(t.geometry, existing.geometry)) / ST_Area(t.geometry) > 0.5
    ) AS first_detection
FROM batch_data_to_insert t;
;

-- Step 3 : Extraire les infos sur les données construites
-- /!\ parametres geocustomzone_id, et attention pour les évolutions (nb nouvelles parcelles cabanisées, nb parcelles remises en état) il faut saisir les années a comparer 

-- nb objets detectés par categories
select object_type,batch_year, count(*) from detections.tmp_detections_analysis
where geocustomzone_id = {geocustomzone_id}
group by object_type,batch_year 
order by batch_year ;

-- nb parcelles cabanisées
select batch_year, count(distinct(id_parcellaire)) from detections.tmp_detections_analysis
where geocustomzone_id = {geocustomzone_id}
group by batch_year 
order by batch_year ;

-- nb parcelles avec des nouvelles detection
select batch_year, count(distinct(id_parcellaire)) from detections.tmp_detections_analysis
where geocustomzone_id = {geocustomzone_id} and first_detection = true
group by batch_year 
order by batch_year ;

-- nb nouvelles parcelles cabanisées
SELECT count(*) FROM (
	SELECT DISTINCT id_parcellaire
	FROM detections.tmp_detections_analysis
	WHERE batch_year = 2021
	and geocustomzone_id = {geocustomzone_id}
	  AND id_parcellaire IS NOT NULL
	EXCEPT
	SELECT DISTINCT id_parcellaire
	FROM detections.tmp_detections_analysis
	WHERE batch_year = 2018
	and geocustomzone_id = {geocustomzone_id}
	  AND id_parcellaire IS NOT null
	 );
	
-- liste ex : -- cas interessant nouvelle parcelle cabanisée :'34032000CN0116'
select id_parcellaire, batch_year, count(*) from tmp_detections_analysis tda	where id_parcellaire in ('34032000CN0116','34032000BK0084','34032000DV0276','34032000AT0200','34032000BZ0273','34032000AV0081','34032000CT0162')
group by id_parcellaire, batch_year;
	

-- nb parcelles remises en état entre 2018 et 2021
-- selectionner la liste des parcelles cabanisees de 2018 n'etant pas présentes dans la liste parcelles des cabanisées 2021
SELECT count(DISTINCT t2018.id_parcellaire)
FROM detections.tmp_detections_analysis t2018
WHERE t2018.batch_year = 2018
  AND t2018.id_parcellaire IS NOT null
  and t2018.geocustomzone_id = {geocustomzone_id}
  AND NOT EXISTS (
    SELECT 1
    FROM detections.tmp_detections_analysis t2021
    WHERE t2021.batch_year = 2021
      and t2021.geocustomzone_id = {geocustomzone_id}
      AND t2021.id_parcellaire = t2018.id_parcellaire
  );
 
 -- liste ex : -- cas interessant parcelle remise en état '34032000AM0026'
select id_parcellaire, batch_year, count(*) from tmp_detections_analysis tda	where id_parcellaire in ('34032000AB0015','34032000AK0007','34032000AM0026','34032000AM0030','34032000AN0042','34032000AN0049','34032000AS0038')
group by id_parcellaire, batch_year;