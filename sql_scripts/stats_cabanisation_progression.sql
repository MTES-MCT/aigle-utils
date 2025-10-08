-- create stats tables : 
-- DROP TABLE analytics.stats_control_analysis;

CREATE TABLE analytics.stats_control_analysis (
	id serial4 NOT NULL,
	geozone_id int4 NOT NULL,
	"year" int4 NOT NULL,
	nb_controls int4 NOT NULL
);

CREATE TABLE analytics.stats_detections_analysis (
	id int8 NOT NULL,
	score numeric NULL,
	object_type text NULL,
	geometry public.geometry(multipolygon, 4326) NULL,
	geozone_id numeric NULL,
	batch_year numeric NULL,
	id_parcellaire text NULL,
	first_detection bool NULL
);

CREATE TABLE analytics.stats_parcels_analysis (
	id serial4 NOT NULL,
	geozone_id int4 NOT NULL,
	nb_parcels int4 NOT NULL,
	nb_parcels_in_ze int4 NOT NULL,
	CONSTRAINT stats_parcels_analysis_pkey PRIMARY KEY (id)
);


CREATE TABLE analytics.stats_pv_analysis (
	id int8 NOT NULL,
	geozone_id numeric NULL,
	"year" numeric NULL,
	nb_pv numeric NULL
);


CREATE TABLE analytics.stats_status_analysis (
	id serial4 NOT NULL,
	geozone_id int4 NOT NULL,
	"year" int4 NOT NULL,
	nb_status_updates int4 NOT NULL
);

-- use notebook to build intermediate geozone aigle stats

-- create view for metabase dashboards

-- analytics.stats_evolutions_cabanisation source

CREATE OR REPLACE VIEW analytics.stats_evolutions_cabanisation
AS WITH stats_pv_2018_2020 AS (
         SELECT stats_pv_analysis.geozone_id,
            sum(
                CASE
                    WHEN stats_pv_analysis.year >= 2018::numeric AND stats_pv_analysis.year <= 2020::numeric THEN stats_pv_analysis.nb_pv
                    ELSE 0::numeric
                END) AS nb_pv_2018_2020
           FROM analytics.stats_pv_analysis
          GROUP BY stats_pv_analysis.geozone_id
          ORDER BY stats_pv_analysis.geozone_id
        ), stats_pv_2021_2023 AS (
         SELECT stats_pv_analysis.geozone_id,
            sum(
                CASE
                    WHEN stats_pv_analysis.year >= 2021::numeric AND stats_pv_analysis.year <= 2023::numeric THEN stats_pv_analysis.nb_pv
                    ELSE 0::numeric
                END) AS nb_pv_2021_2023
           FROM analytics.stats_pv_analysis
          GROUP BY stats_pv_analysis.geozone_id
          ORDER BY stats_pv_analysis.geozone_id
        ), stats_parcelles_cabanisees_2018 AS (
         SELECT stats_detections_analysis.geozone_id,
            count(DISTINCT stats_detections_analysis.id_parcellaire) AS situation_parcelles_cabanisees_2018
           FROM analytics.stats_detections_analysis
          WHERE stats_detections_analysis.batch_year = 2018::numeric
          GROUP BY stats_detections_analysis.geozone_id
          ORDER BY stats_detections_analysis.geozone_id
        ), stats_parcelles_cabanisees_2021 AS (
         SELECT stats_detections_analysis.geozone_id,
            count(DISTINCT stats_detections_analysis.id_parcellaire) AS situation_parcelles_cabanisees_2021
           FROM analytics.stats_detections_analysis
          WHERE stats_detections_analysis.batch_year = 2021::numeric
          GROUP BY stats_detections_analysis.geozone_id
          ORDER BY stats_detections_analysis.geozone_id
        ), stats_parcelles_cabanisees_2024 AS (
         SELECT stats_detections_analysis.geozone_id,
            count(DISTINCT stats_detections_analysis.id_parcellaire) AS situation_parcelles_cabanisees_2024
           FROM analytics.stats_detections_analysis
          WHERE stats_detections_analysis.batch_year = 2024::numeric
          GROUP BY stats_detections_analysis.geozone_id
          ORDER BY stats_detections_analysis.geozone_id
        ), stats_nouvelles_parcelles_cabanisees_2021 AS (
         SELECT new_parcellaire.geozone_id,
            count(*) AS nouvelles_parcelles_cabanisees_2021_vs_2018
           FROM ( SELECT DISTINCT stats_detections_analysis.id_parcellaire,
                    stats_detections_analysis.geozone_id
                   FROM analytics.stats_detections_analysis
                  WHERE stats_detections_analysis.batch_year = 2021::numeric AND stats_detections_analysis.id_parcellaire IS NOT NULL
                EXCEPT
                 SELECT DISTINCT stats_detections_analysis.id_parcellaire,
                    stats_detections_analysis.geozone_id
                   FROM analytics.stats_detections_analysis
                  WHERE stats_detections_analysis.batch_year = 2018::numeric AND stats_detections_analysis.id_parcellaire IS NOT NULL) new_parcellaire
          GROUP BY new_parcellaire.geozone_id
          ORDER BY new_parcellaire.geozone_id
        ), stats_nouvelles_parcelles_cabanisees_2024 AS (
         SELECT new_parcellaire.geozone_id,
            count(*) AS nouvelles_parcelles_cabanisees_2024_vs_2021
           FROM ( SELECT DISTINCT stats_detections_analysis.id_parcellaire,
                    stats_detections_analysis.geozone_id
                   FROM analytics.stats_detections_analysis
                  WHERE stats_detections_analysis.batch_year = 2024::numeric AND stats_detections_analysis.id_parcellaire IS NOT NULL
                EXCEPT
                 SELECT DISTINCT stats_detections_analysis.id_parcellaire,
                    stats_detections_analysis.geozone_id
                   FROM analytics.stats_detections_analysis
                  WHERE (stats_detections_analysis.batch_year = ANY (ARRAY[2021::numeric, 2018::numeric])) AND stats_detections_analysis.id_parcellaire IS NOT NULL) new_parcellaire
          GROUP BY new_parcellaire.geozone_id
          ORDER BY new_parcellaire.geozone_id
        ), stats_parcelles_remises_en_etat_2021 AS (
         SELECT t2018.geozone_id,
            count(DISTINCT t2018.id_parcellaire) AS parcelles_remises_en_etat_2021_vs_2018
           FROM analytics.stats_detections_analysis t2018
          WHERE t2018.batch_year = 2018::numeric AND t2018.id_parcellaire IS NOT NULL AND NOT (EXISTS ( SELECT 1
                   FROM analytics.stats_detections_analysis t2021
                  WHERE t2021.batch_year = 2021::numeric AND t2021.id_parcellaire = t2018.id_parcellaire AND t2021.geozone_id = t2018.geozone_id))
          GROUP BY t2018.geozone_id
          ORDER BY t2018.geozone_id
        ), stats_parcelles_remises_en_etat_2024 AS (
         SELECT t2021.geozone_id,
            count(DISTINCT t2021.id_parcellaire) AS parcelles_remises_en_etat_2024_vs_2021
           FROM analytics.stats_detections_analysis t2021
          WHERE t2021.batch_year = 2021::numeric AND t2021.id_parcellaire IS NOT NULL AND NOT (EXISTS ( SELECT 1
                   FROM analytics.stats_detections_analysis t2024
                  WHERE t2024.batch_year = 2024::numeric AND t2024.id_parcellaire = t2021.id_parcellaire AND t2024.geozone_id = t2021.geozone_id))
          GROUP BY t2021.geozone_id
          ORDER BY t2021.geozone_id
        )
 SELECT fgr.department_name,
    fgr.geozone_code,
    stats_pv_2021_2023.geozone_id,
    stats_pv_2021_2023.nb_pv_2021_2023 AS nb_pv_period,
    stats_parcelles_cabanisees_2021.situation_parcelles_cabanisees_2021 AS situation_parcelles_cabanisees_debut_periode,
    stats_parcelles_cabanisees_2024.situation_parcelles_cabanisees_2024 AS situation_parcelles_cabanisees_fin_periode,
    stats_nouvelles_parcelles_cabanisees_2024.nouvelles_parcelles_cabanisees_2024_vs_2021 AS situation_nouvelles_parcelles_cabanisees_periode,
    stats_parcelles_remises_en_etat_2024.parcelles_remises_en_etat_2024_vs_2021 AS situation_parcelles_remises_en_etat_periode,
        CASE
            WHEN fgr.department_name::text = 'gard'::text THEN 'lutte avec données aigle'::text
            WHEN fgr.id = ANY (ARRAY[278::bigint, 171::bigint, 259::bigint, 195::bigint, 261::bigint, 138::bigint, 313::bigint, 281::bigint, 330::bigint, 118::bigint, 255::bigint, 111::bigint, 288::bigint, 226::bigint, 248::bigint]) THEN 'pas d engagement'::text
            ELSE 'lutte avec aigle'::text
        END AS groupe,
    '2021-2023'::text AS periode,
    spa.nb_parcels,
    spa.nb_parcels_in_ze
   FROM stats_pv_2021_2023
     LEFT JOIN stats_parcelles_cabanisees_2021 ON stats_pv_2021_2023.geozone_id = stats_parcelles_cabanisees_2021.geozone_id
     LEFT JOIN stats_parcelles_cabanisees_2024 ON stats_pv_2021_2023.geozone_id = stats_parcelles_cabanisees_2024.geozone_id
     LEFT JOIN stats_nouvelles_parcelles_cabanisees_2024 ON stats_pv_2021_2023.geozone_id = stats_nouvelles_parcelles_cabanisees_2024.geozone_id
     LEFT JOIN stats_parcelles_remises_en_etat_2024 ON stats_pv_2021_2023.geozone_id = stats_parcelles_remises_en_etat_2024.geozone_id
     LEFT JOIN analytics.stats_mapping_preprod_geozone_id fgr ON stats_pv_2021_2023.geozone_id = fgr.id::numeric
     LEFT JOIN analytics.stats_parcels_analysis spa ON stats_pv_2021_2023.geozone_id = spa.geozone_id::numeric
  WHERE stats_parcelles_cabanisees_2021.situation_parcelles_cabanisees_2021 IS NOT NULL
UNION
 SELECT fgr.department_name,
    fgr.geozone_code,
    stats_pv_2018_2020.geozone_id,
    stats_pv_2018_2020.nb_pv_2018_2020 AS nb_pv_period,
    stats_parcelles_cabanisees_2018.situation_parcelles_cabanisees_2018 AS situation_parcelles_cabanisees_debut_periode,
    stats_parcelles_cabanisees_2021.situation_parcelles_cabanisees_2021 AS situation_parcelles_cabanisees_fin_periode,
    stats_nouvelles_parcelles_cabanisees_2021.nouvelles_parcelles_cabanisees_2021_vs_2018 AS situation_nouvelles_parcelles_cabanisees_periode,
    stats_parcelles_remises_en_etat_2021.parcelles_remises_en_etat_2021_vs_2018 AS situation_parcelles_remises_en_etat_periode,
        CASE
            WHEN fgr.id = ANY (ARRAY[278::bigint, 171::bigint, 259::bigint, 195::bigint, 261::bigint, 138::bigint, 313::bigint, 281::bigint, 330::bigint, 118::bigint, 255::bigint, 111::bigint, 288::bigint, 226::bigint, 248::bigint]) THEN 'pas d engagement'::text
            ELSE 'lutte sans outils'::text
        END AS groupe,
    '2018-2020'::text AS periode,
    spa.nb_parcels,
    spa.nb_parcels_in_ze
   FROM stats_pv_2018_2020
     LEFT JOIN stats_parcelles_cabanisees_2018 ON stats_pv_2018_2020.geozone_id = stats_parcelles_cabanisees_2018.geozone_id
     LEFT JOIN stats_parcelles_cabanisees_2021 ON stats_pv_2018_2020.geozone_id = stats_parcelles_cabanisees_2021.geozone_id
     LEFT JOIN stats_nouvelles_parcelles_cabanisees_2021 ON stats_pv_2018_2020.geozone_id = stats_nouvelles_parcelles_cabanisees_2021.geozone_id
     LEFT JOIN stats_parcelles_remises_en_etat_2021 ON stats_pv_2018_2020.geozone_id = stats_parcelles_remises_en_etat_2021.geozone_id
     LEFT JOIN analytics.stats_mapping_preprod_geozone_id fgr ON stats_pv_2018_2020.geozone_id = fgr.id::numeric
     LEFT JOIN analytics.stats_parcels_analysis spa ON stats_pv_2018_2020.geozone_id = spa.geozone_id::numeric
  WHERE stats_parcelles_cabanisees_2018.situation_parcelles_cabanisees_2018 IS NOT NULL;



CREATE MATERIALIZED VIEW detections.stats_epci_evolutions_cabanisation_raw
TABLESPACE pg_default
AS 

WITH stats_parcelles_cabanisees_2018 AS (
    SELECT 
        geozone_id,
        COUNT(DISTINCT id_parcellaire) AS situation_parcelles_cabanisees_2018
    FROM detections.stats_detections_epci_analysis
    WHERE batch_year = 2018::numeric
      AND id_parcellaire IS NOT NULL
    GROUP BY geozone_id
), 

stats_parcelles_cabanisees_2021 AS (
    SELECT 
        geozone_id,
        COUNT(DISTINCT id_parcellaire) AS situation_parcelles_cabanisees_2021
    FROM detections.stats_detections_epci_analysis
    WHERE batch_year = 2021::numeric
      AND id_parcellaire IS NOT NULL
    GROUP BY geozone_id
), 

stats_parcelles_cabanisees_2024 AS (
    SELECT 
        geozone_id,
        COUNT(DISTINCT id_parcellaire) AS situation_parcelles_cabanisees_2024
    FROM detections.stats_detections_epci_analysis
    WHERE batch_year = 2024::numeric
      AND id_parcellaire IS NOT NULL
    GROUP BY geozone_id
), 

stats_nouvelles_parcelles_cabanisees_2021 AS (
    SELECT 
        endp.geozone_id,
        COUNT(DISTINCT endp.id_parcellaire) AS nouvelles_parcelles_cabanisees_2021_vs_2018
    FROM detections.stats_detections_epci_analysis endp
    LEFT JOIN detections.stats_detections_epci_analysis startp
      ON endp.id_parcellaire = startp.id_parcellaire
     AND endp.geozone_id = startp.geozone_id
     AND startp.batch_year = 2018
    WHERE endp.batch_year = 2021
      AND startp.id_parcellaire IS NULL
    GROUP BY endp.geozone_id
), 

stats_nouvelles_parcelles_cabanisees_2024 AS (
    SELECT 
        endp.geozone_id,
        COUNT(DISTINCT endp.id_parcellaire) AS nouvelles_parcelles_cabanisees_2024_vs_2021
    FROM detections.stats_detections_epci_analysis endp
    LEFT JOIN detections.stats_detections_epci_analysis startp
      ON endp.id_parcellaire = startp.id_parcellaire
     AND endp.geozone_id = startp.geozone_id
     AND startp.batch_year = 2021
    WHERE endp.batch_year = 2024
      AND startp.id_parcellaire IS NULL
    GROUP BY endp.geozone_id
), 

stats_parcelles_remises_en_etat_2021 AS (
    SELECT 
        startp.geozone_id,
        COUNT(DISTINCT startp.id_parcellaire) AS parcelles_remises_en_etat_2021_vs_2018
    FROM detections.stats_detections_epci_analysis startp
    LEFT JOIN detections.stats_detections_epci_analysis endp
      ON startp.id_parcellaire = endp.id_parcellaire
     AND startp.geozone_id = endp.geozone_id
     AND endp.batch_year = 2021
    WHERE startp.batch_year = 2018
      AND endp.id_parcellaire IS NULL
    GROUP BY startp.geozone_id
), 

stats_parcelles_remises_en_etat_2024 AS (
    SELECT 
        startp.geozone_id,
        COUNT(DISTINCT startp.id_parcellaire) AS parcelles_remises_en_etat_2024_vs_2021
    FROM detections.stats_detections_epci_analysis startp
    LEFT JOIN detections.stats_detections_epci_analysis endp
      ON startp.id_parcellaire = endp.id_parcellaire
     AND startp.geozone_id = endp.geozone_id
     AND endp.batch_year = 2024
    WHERE startp.batch_year = 2021
      AND endp.id_parcellaire IS NULL
    GROUP BY startp.geozone_id
)

SELECT 
    cgz.name_normalized,
    stats_parcelles_cabanisees_2021.geozone_id,
    COALESCE(stats_parcelles_cabanisees_2021.situation_parcelles_cabanisees_2021, 0) AS situation_parcelles_cabanisees_debut_periode,
    COALESCE(stats_parcelles_cabanisees_2024.situation_parcelles_cabanisees_2024, 0) AS situation_parcelles_cabanisees_fin_periode,
    COALESCE(stats_nouvelles_parcelles_cabanisees_2024.nouvelles_parcelles_cabanisees_2024_vs_2021, 0) AS situation_nouvelles_parcelles_cabanisees_periode,
    COALESCE(stats_parcelles_remises_en_etat_2024.parcelles_remises_en_etat_2024_vs_2021, 0) AS situation_parcelles_remises_en_etat_periode,
    '2021-2023'::text AS periode,
    COALESCE(spa.nb_parcels, 0) AS nb_parcels,
    COALESCE(spa.nb_parcels_in_ze, 0) AS nb_parcels_in_ze
FROM stats_parcelles_cabanisees_2021
LEFT JOIN core_geozone cgz ON stats_parcelles_cabanisees_2021.geozone_id = cgz.id::numeric
LEFT JOIN stats_parcelles_cabanisees_2024 ON stats_parcelles_cabanisees_2021.geozone_id = stats_parcelles_cabanisees_2024.geozone_id
LEFT JOIN stats_nouvelles_parcelles_cabanisees_2024 ON stats_parcelles_cabanisees_2021.geozone_id = stats_nouvelles_parcelles_cabanisees_2024.geozone_id
LEFT JOIN stats_parcelles_remises_en_etat_2024 ON stats_parcelles_cabanisees_2021.geozone_id = stats_parcelles_remises_en_etat_2024.geozone_id
LEFT JOIN detections.stats_epci_parcels_analysis spa ON stats_parcelles_cabanisees_2021.geozone_id = spa.geozone_id::numeric
WHERE stats_parcelles_cabanisees_2021.situation_parcelles_cabanisees_2021 IS NOT NULL

UNION ALL

SELECT 
    cgz.name_normalized,
    stats_parcelles_cabanisees_2018.geozone_id,
    COALESCE(stats_parcelles_cabanisees_2018.situation_parcelles_cabanisees_2018, 0) AS situation_parcelles_cabanisees_debut_periode,
    COALESCE(stats_parcelles_cabanisees_2021.situation_parcelles_cabanisees_2021, 0) AS situation_parcelles_cabanisees_fin_periode,
    COALESCE(stats_nouvelles_parcelles_cabanisees_2021.nouvelles_parcelles_cabanisees_2021_vs_2018, 0) AS situation_nouvelles_parcelles_cabanisees_periode,
    COALESCE(stats_parcelles_remises_en_etat_2021.parcelles_remises_en_etat_2021_vs_2018, 0) AS situation_parcelles_remises_en_etat_periode,
    '2018-2020'::text AS periode,
    COALESCE(spa.nb_parcels, 0) AS nb_parcels,
    COALESCE(spa.nb_parcels_in_ze, 0) AS nb_parcels_in_ze
FROM stats_parcelles_cabanisees_2018
LEFT JOIN core_geozone cgz ON stats_parcelles_cabanisees_2018.geozone_id = cgz.id::numeric
LEFT JOIN stats_parcelles_cabanisees_2021 ON stats_parcelles_cabanisees_2018.geozone_id = stats_parcelles_cabanisees_2021.geozone_id
LEFT JOIN stats_nouvelles_parcelles_cabanisees_2021 ON stats_parcelles_cabanisees_2018.geozone_id = stats_nouvelles_parcelles_cabanisees_2021.geozone_id
LEFT JOIN stats_parcelles_remises_en_etat_2021 ON stats_parcelles_cabanisees_2018.geozone_id = stats_parcelles_remises_en_etat_2021.geozone_id
LEFT JOIN detections.stats_epci_parcels_analysis spa ON stats_parcelles_cabanisees_2018.geozone_id = spa.geozone_id::numeric
WHERE stats_parcelles_cabanisees_2018.situation_parcelles_cabanisees_2018 IS NOT NULL

WITH DATA;

CREATE MATERIALIZED VIEW detections.stats_epci_evolutions_cabanisation
TABLESPACE pg_default
AS SELECT name_normalized,
    geozone_id,
    floor(situation_parcelles_cabanisees_debut_periode::numeric * 0.705 * (1::numeric - 0.252)) AS situation_parcelles_cabanisees_debut_periode,
    floor(situation_parcelles_cabanisees_fin_periode::numeric * 0.705 * (1::numeric - 0.252)) AS situation_parcelles_cabanisees_fin_periode,
    floor(situation_nouvelles_parcelles_cabanisees_periode::numeric * 0.705 * (1::numeric - 0.252)) AS situation_nouvelles_parcelles_cabanisees_periode,
    floor(situation_parcelles_remises_en_etat_periode::numeric * 0.705 * (1::numeric - 0.252)) AS situation_parcelles_remises_en_etat_periode,
    periode,
    nb_parcels,
    nb_parcels_in_ze
   FROM detections.stats_epci_evolutions_cabanisation_raw
WITH DATA;

CREATE materialized view detections.stats_epci_coords
TABLESPACE pg_default
	AS 
	SELECT 
	    id,
	    name,
	    ST_Centroid(geometry)::geometry(Point, 4326) AS barycenter_coords
	FROM public.core_geozone
	WHERE id IN (
	    37087,36513,37374,37380,36519,37373,37378,37375,
	    37377,37376,36802,36803,37379,37372,36717,36716
	)
with DATA;

-- analytics.stats_departments_coords source
CREATE MATERIALIZED VIEW analytics.stats_departments_coords
TABLESPACE pg_default
AS
SELECT
    id,
    name,
    ST_Centroid(geometry)::geometry(Point, 4326) AS barycenter_coords,
    CASE
        WHEN id IN (36155, 36156, 36160, 36161)
            THEN CASE id
                WHEN 36155 THEN 51.0
                WHEN 36156 THEN 49.0
                WHEN 36160 THEN 45.0
                WHEN 36161 THEN 43.0
            END
        ELSE ST_Y(ST_Centroid(geometry)::geometry(Point, 4326))
    END AS lat,
    CASE
        WHEN id IN (36155, 36156, 36160, 36161)
            THEN -7.223655
        ELSE ST_X(ST_Centroid(geometry)::geometry(Point, 4326))
    END AS lon
FROM public.core_geozone
WHERE id IN (
    SELECT cg.geozone_ptr_id
    FROM public.core_geodepartment cg
    WHERE core_geozone.name <> 'Guyane'
)
WITH DATA;

-- analytics.stats_communes_parcels_status source

CREATE MATERIALIZED VIEW analytics.stats_communes_parcels_status
TABLESPACE pg_default
AS WITH parcel_flags AS (
         SELECT cd.tile_set_id,
            cd3.commune_id,
            cd3.parcel_id,
            bool_or(cd2.detection_validation_status::text = 'SUSPECT'::text) AS has_suspect,
            bool_or(cd2.detection_validation_status::text = 'LEGITIMATE'::text) AS has_legitimate,
            bool_or(cd2.detection_validation_status::text = 'DETECTED_NOT_VERIFIED'::text) AS has_dnv
           FROM core_detection cd
             LEFT JOIN core_detectiondata cd2 ON cd.detection_data_id = cd2.id
             LEFT JOIN core_detectionobject cd3 ON cd.detection_object_id = cd3.id
             LEFT JOIN core_objecttype co ON cd3.object_type_id = co.id
             LEFT JOIN core_geozone cg_1 ON cd3.commune_id = cg_1.id
          WHERE (cd.tile_set_id = ANY (ARRAY[1::bigint, 2::bigint, 17::bigint])) AND (co.name::text = ANY (ARRAY['Construction en dur'::character varying, 'Piscine'::character varying, 'Mobil-home'::character varying, 'Caravane'::character varying, 'Installation Legere'::character varying]::text[])) AND (st_within(st_setsrid(cd.geometry, 4326), ( SELECT geozone.geometry
                   FROM core_geozone geozone
                  WHERE geozone.id = 351)) OR st_within(st_setsrid(cd.geometry, 4326), ( SELECT geozone.geometry
                   FROM core_geozone geozone
                  WHERE geozone.id = 352)) OR st_within(st_setsrid(cd.geometry, 4326), ( SELECT geozone.geometry
                   FROM core_geozone geozone
                  WHERE geozone.id = 361))) AND (cd.tile_set_id = 1 AND cd2.detection_validation_status::text = 'DETECTED_NOT_VERIFIED'::text AND cd.score > ((1::numeric - 0.3) * 0.6)::double precision AND (cd2.detection_prescription_status IS NULL OR cd2.detection_prescription_status::text <> 'PRESCRIBED'::text) OR cd.tile_set_id = 2 AND cd2.detection_validation_status::text = 'DETECTED_NOT_VERIFIED'::text AND cd.score > ((1::numeric - 0.3) * 0.3)::double precision AND (cd2.detection_prescription_status IS NULL OR cd2.detection_prescription_status::text <> 'PRESCRIBED'::text) OR cd.tile_set_id = 17 AND cd2.detection_validation_status::text = 'DETECTED_NOT_VERIFIED'::text AND cd.score > 0.3::double precision AND (cd2.detection_prescription_status IS NULL OR cd2.detection_prescription_status::text <> 'PRESCRIBED'::text) OR cd2.detection_validation_status::text = 'SUSPECT'::text OR cd2.detection_validation_status::text = 'LEGITIMATE'::text)
          GROUP BY cd.tile_set_id, cd3.commune_id, cd3.parcel_id
        )
 SELECT parcel_flags.tile_set_id,
    parcel_flags.commune_id,
    cg.name AS commune_name,
    count(DISTINCT parcel_flags.parcel_id) FILTER (WHERE parcel_flags.has_suspect) AS parcels_with_suspect,
    count(DISTINCT parcel_flags.parcel_id) FILTER (WHERE parcel_flags.has_dnv) AS parcels_with_dnv,
    count(DISTINCT parcel_flags.parcel_id) FILTER (WHERE parcel_flags.has_legitimate) AS parcels_with_legit,
    count(DISTINCT parcel_flags.parcel_id) FILTER (WHERE NOT parcel_flags.has_suspect AND (parcel_flags.has_legitimate OR parcel_flags.has_dnv)) AS parcels_with_legit_and_dnv_no_suspect
   FROM parcel_flags
     LEFT JOIN core_geozone cg ON parcel_flags.commune_id = cg.id
  GROUP BY cg.name, parcel_flags.tile_set_id, parcel_flags.commune_id
  ORDER BY cg.name, parcel_flags.tile_set_id, parcel_flags.commune_id
WITH DATA;


CREATE MATERIALIZED VIEW analytics.stats_deployement_s2_2025 AS
WITH aigle_deployed AS (
    SELECT 
        cug.name,
        cug.user_group_type,
        cugz.geozone_id,
        cgzone.iso_code AS insee_code,
        CASE 
            WHEN cug.name = 'Cabanisation Pic Saint Loup' THEN '34'
            ELSE cgdep.insee_code
        END AS dep_code,
        CASE 
            WHEN cug.name = 'Cabanisation Pic Saint Loup' THEN 2
            ELSE cgdep.geozone_ptr_id
        END AS dep_geozone_id
    FROM public.core_usergroup cug
    LEFT JOIN public.core_usergroup_geo_zones cugz 
        ON cug.id = cugz.usergroup_id
    LEFT JOIN public.core_geocommune cgzone 
        ON cugz.geozone_id = cgzone.geozone_ptr_id
    LEFT JOIN public.core_geodepartment cgdep 
        ON cgzone.department_id = cgdep.geozone_ptr_id
    WHERE 
        cugz.geozone_id IS NOT NULL
        AND cug.user_group_type = 'COLLECTIVITY'
        AND cugz.geozone_id <> 1613
),
agg AS (
    SELECT 
        dep_geozone_id, 
        COUNT(DISTINCT name) AS nb_collectivity
    FROM aigle_deployed
    GROUP BY dep_geozone_id
),
synthetic AS (
    SELECT * FROM (VALUES
        (1540, 0),
        (1547, 0),
        (1551, 0),
        (1580, 0),
        (36156, 0)
    ) AS s(dep_geozone_id, nb_collectivity)
)
SELECT a.dep_geozone_id, a.nb_collectivity
FROM agg a

UNION ALL

SELECT s.dep_geozone_id, s.nb_collectivity
FROM synthetic s
WHERE s.dep_geozone_id NOT IN (
    SELECT dep_geozone_id FROM agg
)
WITH DATA;