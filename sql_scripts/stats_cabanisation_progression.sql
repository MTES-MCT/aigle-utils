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