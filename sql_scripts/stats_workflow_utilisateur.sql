/*
    DEMARCHE :
    - identifier et modeliser le workflow lucca
    - identifier et modeliser le workflow aigle
    - construire le workflow general
    - identifier les caracteristiques et congestions du workflow dans l'usage : durée des transitions, voies impasses, dossiers non terminés
    - adapter en flux parcelles


*/

-- 1. creation workflow LUCCA
CREATE TABLE analytics.workflow_steps_lucca (
    step_code VARCHAR(10) PRIMARY KEY,
    step_order INT NOT NULL,
    step_label VARCHAR(255) NOT NULL
);
INSERT INTO analytics.workflow_steps_lucca (step_code, step_order, step_label) VALUES
('1',   1, 'Ouverture dossier'),
('2A',  2, 'Création contrôle avec droit de visite'),
('2B',  2, 'Création contrôle sans droit visite'),
('3A',  3, 'Création PV avec natinfs'),
('3B',  3, 'Création rapport de constatation (PV sans natinfs)'),
('4',   4, 'Création courrier'), -- (a destination du maire ou a destination du procureur, a t on l'info en BD? @lucca)
('5',   5, 'Création PV reactualisation'),
('6',   6, 'Création décisions de justice'),
('7A',  7, 'Clôture dossier avec remise en état'),
('7B',  7, 'Clôture dossier autres raisons'); -- regularisation


CREATE TABLE analytics.workflow_transitions_lucca (
    from_step VARCHAR(10) NOT NULL,
    to_step   VARCHAR(10) NOT NULL,
    PRIMARY KEY (from_step, to_step),
    FOREIGN KEY (from_step) REFERENCES analytics.workflow_steps_lucca(step_code),
    FOREIGN KEY (to_step)   REFERENCES analytics.workflow_steps_lucca(step_code)
);

INSERT INTO analytics.workflow_transitions_lucca (from_step, to_step) VALUES
-- Ouverture dossier → contrôles
('1', '2A'),
('1', '2B'),
-- Contrôles → PV
('2A', '3A'),
('2A', '3B'),
('2A', '7A'),
('2B', '3A'),
('2B', '3B'),
('2B', '7A'),

-- PV → PV reactualisation or courrier
('3A', '4'),
('3A', '5'),
('3B', '4'),
('3B', '5'),

-- Courrier →  reactualisation, justice decision
('4', '6'),
('4', '5'),

-- reactalisation -> PV
('5', '2A'),
('5', '2B'),

-- Justice → Clôture
('6','5'),
('6', '7A'),
('6', '7B');


create view tracked_changes_lucca_v1 as
with ordered_changes as (
select
    stats_history.id AS id,
    stats_history.dossier_id AS dossier_id,
    stats_history.departement AS departement,
    stats_history.ville AS ville,
    stats_history.action_type AS action_type,
    stats_history.action_date AS action_date,
    lead(stats_history.action_type) OVER (PARTITION BY stats_history.dossier_id
ORDER BY
    stats_history.id ) AS next_action_type,
    lead(stats_history.action_date) OVER (PARTITION BY stats_history.dossier_id
ORDER BY
    stats_history.id ) AS next_action_date
from
    stats_history
where
    (stats_history.ville = 'MARSEILLAN')
)
select
    oe.id AS id,
    oe.dossier_id AS dossier_id,
    oe.departement AS departement,
    oe.ville AS ville,
    oe.action_type AS action_type,
    oe.action_date AS action_date,
    oe.next_action_type AS next_action_type,
    oe.next_action_date AS next_action_date,
    ws_from.step_code AS source_step,
    ws_to.step_code AS target_step
from
    ((ordered_changes oe
left join workflow_steps ws_from on
    (((oe.action_type collate utf8mb4_unicode_ci) = ws_from.step_label)))
left join workflow_steps ws_to on
    (((oe.next_action_type collate utf8mb4_unicode_ci) = ws_to.step_label)))
where next_action_date is not null;


-- 2. creation workflow aigle 

-- a. aggreger une vue historique sur l'import et sur l'app
-- selectionner les actions liées a l'import (detectées par history_type = '+')
-- selectionner les actions liées a l'import (detectées par history_type = '+')
CREATE MATERIALIZED VIEW analytics.tracked_import_changes_30169_ze_2024_v1 AS
with "track_import_changes" as (
   SELECT distinct on (detection_id, detection_data_id, history_id) ch.id as detection_id,
   chdata.id as detection_data_id,
   ch.history_id,
   ch.history_date,
   cho.parcel_id, 
   'admin_import@beta.gouv.fr' as "email",
   'Admin import' as "group_name",
   chdata.changed_fields,
   'IMPORTED' as "old_pre_status",
   'IMPORTED' as "old_val_status",
   'IMPORTED' as "old_ctl_status",
   (case when chdata.detection_prescription_status is null and ch.auto_prescribed is not true then 'NOT_PRESCRIBED' 
   		when chdata.detection_prescription_status is null and ch.auto_prescribed is true then 'PRESCRIBED' 
   		else chdata.detection_prescription_status END) as "new_pre_status",
   (case when chdata.detection_validation_status is null then 'DETECTED_NOT_VERIFIED' else chdata.detection_validation_status end) as "new_val_status",
   (case when chdata.detection_control_status is null then 'NOT_CONTROLLED' else chdata.detection_control_status end) as "new_ctl_status"
   FROM core_historicaldetectiondata chdata
     LEFT JOIN core_historicaldetection ch ON chdata.id = ch.detection_data_id
     LEFT JOIN core_historicaldetectionobject cho ON ch.detection_object_id = cho.id
   WHERE ch.tile_set_id = 19 
	  AND st_within(ch.geometry, ( SELECT core_geozone.geometry
	           FROM core_geozone
	          WHERE core_geozone.id = 979)) 
	  AND (st_within(ch.geometry, ( SELECT core_geozone.geometry
	           FROM core_geozone
	          WHERE core_geozone.id = 1520)) OR st_within(ch.geometry, ( SELECT core_geozone.geometry
	           FROM core_geozone
	          WHERE core_geozone.id = 1521)) OR st_within(ch.geometry, ( SELECT core_geozone.geometry
	           FROM core_geozone
	          WHERE core_geozone.id = 1522))) 
	  AND ch.score > 0.3::double precision
	  -- we consider that import is done in two step, first insertion then autoprescription control
	  and ((chdata.history_type = '+' and ch.history_type = '+') or (ch.history_type = '~' and ch.auto_prescribed is true and chdata.changed_fields is null ))
	  )
select distinct on (detection_data_id, old_status) track_import_changes.*,
	old_val_status || '.' || old_ctl_status || '.' || old_pre_status  as "old_status",
   new_val_status || '.' || new_ctl_status || '.' || new_pre_status as "new_status"
from track_import_changes
-- to keep only the last history of import with history_id DESC combined to distinct on only (detection_data_id, old_status)
order by detection_data_id, old_status, history_id DESC
with data;


-- analytics.tracked_changes_30169_ze_2024_v1 source
-- analytics.tracked_app_changes_30169_ze_2024_v1 source

CREATE MATERIALIZED VIEW analytics.tracked_app_changes_30169_ze_2024_v1
TABLESPACE pg_default
AS WITH base AS (
         select distinct on (ch.id, chdata.id, chdata.history_id) ch.id AS detection_id,
            chdata.id AS detection_data_id,
            chdata.changed_fields,
            chdata.detection_validation_status,
            chdata.detection_control_status,
            chdata.detection_prescription_status,
            chdata.history_id,
            chdata.history_date,
            cho.parcel_id,
            users.email,
            users.group_name
           FROM core_historicaldetectiondata chdata
             LEFT JOIN core_historicaldetection ch ON chdata.id = ch.detection_data_id
             JOIN users ON users.id = chdata.history_user_id
             LEFT JOIN core_historicaldetectionobject cho ON ch.detection_object_id = cho.id
             LEFT JOIN core_usergroup ON users.group_id = core_usergroup.id
          WHERE ch.tile_set_id = 19 AND st_within(ch.geometry, ( SELECT core_geozone.geometry
                   FROM core_geozone
                  WHERE core_geozone.id = 979)) AND (st_within(ch.geometry, ( SELECT core_geozone.geometry
                   FROM core_geozone
                  WHERE core_geozone.id = 1520)) OR st_within(ch.geometry, ( SELECT core_geozone.geometry
                   FROM core_geozone
                  WHERE core_geozone.id = 1521)) OR st_within(ch.geometry, ( SELECT core_geozone.geometry
                   FROM core_geozone
                  WHERE core_geozone.id = 1522))) AND ch.score > 0.3::double precision
                  and chdata.changed_fields is not null
          order by ch.id, chdata.id, chdata.history_id, cho.parcel_id DESC
), 
changes AS (
         SELECT b.detection_id,
            b.detection_data_id,
            b.changed_fields,
            b.detection_validation_status,
            b.detection_control_status,
            b.detection_prescription_status,
            b.history_id,
            b.history_date,
            b.parcel_id,
            b.email,
            b.group_name,
            elem.value ->> 'field'::text AS field_name,
            elem.value ->> 'old_value'::text AS old_value_raw,
            elem.value ->> 'new_value'::text AS new_value_raw
           FROM base b,
            LATERAL jsonb_array_elements(b.changed_fields) elem(value)
          WHERE (elem.value ->> 'field'::text) = ANY (ARRAY['detection_validation_status'::text, 'detection_control_status'::text, 'detection_prescription_status'::text])
), 
grouped_changes AS (
         SELECT changes.detection_id,
            changes.detection_data_id,
            changes.history_id,
            changes.history_date,
            changes.parcel_id,
            changes.email,
            changes.group_name,
            jsonb_agg(jsonb_build_object('field', changes.field_name, 'old', changes.old_value_raw, 'new', changes.new_value_raw)) AS fields,
            max(changes.detection_validation_status::text) AS new_val_status,
            max(changes.detection_control_status::text) AS new_ctl_status,
            max(changes.detection_prescription_status::text) AS new_pre_status
           FROM changes
          where detection_id = 5989456
          GROUP BY changes.detection_id, changes.detection_data_id, changes.history_id, changes.history_date, changes.parcel_id, changes.email, changes.group_name
),
cleaned_changes AS (
    SELECT
        *,
         -- extract old values from JSONB array if present
	        (SELECT f->>'old'
	         FROM jsonb_array_elements(fields) f
	         WHERE f->>'field' = 'detection_validation_status'
	        ) AS old_val_status,
	
	        (SELECT f->>'old'
	         FROM jsonb_array_elements(fields) f
	         WHERE f->>'field' = 'detection_control_status'
	        ) AS old_ctl_status,
	
	        (SELECT f->>'old'
	         FROM jsonb_array_elements(fields) f
	         WHERE f->>'field' = 'detection_prescription_status'
	        ) AS old_pre_status
    FROM grouped_changes
)    
 SELECT detection_id,
    detection_data_id,
    history_id,
    history_date,
    parcel_id,
    email,
    group_name,
    fields,
    new_val_status,
    new_ctl_status,
    new_pre_status,
    COALESCE(old_val_status, new_val_status) as old_val_status, -- if new is null mean no changes so we get old status
    COALESCE(old_ctl_status,new_ctl_status) as old_ctl_status, -- if new is null mean no changes so we get old status
    COALESCE(old_pre_status, new_pre_status) as old_pre_status, -- if new is null mean no changes so we get old status
	COALESCE(old_val_status, new_val_status) ||'.' || COALESCE(old_ctl_status,new_ctl_status) || '.' || COALESCE(old_pre_status, new_pre_status)  AS old_status,
    new_val_status ||'.' || new_ctl_status || '.' || new_pre_status AS new_status
   FROM cleaned_changes
with data;


-- b. mettre en place les contraintes metier

CREATE TABLE analytics.workflow_steps (
    step_code VARCHAR(10) PRIMARY KEY,
    step_order INT NOT NULL,
    step_label VARCHAR(255) NOT NULL
);
INSERT INTO analytics.workflow_steps (step_code, step_order, step_label) VALUES
('0',   0, 'Importé'),
('1',   1, 'Détecté non vérifié'),
('2A',  2, 'Suspect non controlé'),
('2B',  2, 'Invalidé'),
('2C',  2, 'Légitime'),
('3A',  3, 'Non controlé - obsolete'),
('3B',  3, 'Prescrit'),
('4',  4, 'Courrier préalable envoyé'),
('5',  5, 'Controlé terrain'),
('6A',  6, 'PV Dressé'),
('6B',  6, 'Rapport de constatation rédigé'),
('7',   7, 'En astreinte administrative'),
('8',  8, 'Remis en état');


CREATE TABLE analytics.workflow_steps_mapping_status (
  step_code varchar(10) NOT NULL,
  status_code varchar(255) NOT NULL
)


INSERT INTO analytics.workflow_steps_mapping_status (step_code, status_code) VALUES
-- v2
('1', 'DETECTED_NOT_VERIFIED.NOT_CONTROLLED.NOT_PRESCRIBED'),
('3B', 'DETECTED_NOT_VERIFIED.NOT_CONTROLLED.PRESCRIBED'),
('6A', 'DETECTED_NOT_VERIFIED.OFFICIAL_REPORT_DRAWN_UP.NOT_PRESCRIBED'),
('0', 'IMPORTED.IMPORTED.IMPORTED'),
('2B', 'INVALIDATED.NOT_CONTROLLED.NOT_PRESCRIBED'),
('2C', 'LEGITIMATE.NOT_CONTROLLED.NOT_PRESCRIBED'),
('2C', 'LEGITIMATE.OBSERVARTION_REPORT_REDACTED.NOT_PRESCRIBED'),
('5', 'SUSPECT.CONTROLLED_FIELD.NOT_PRESCRIBED'),
('3B', 'SUSPECT.CONTROLLED_FIELD.PRESCRIBED'),
('2A', 'SUSPECT.NOT_CONTROLLED.NOT_PRESCRIBED'),
('3B', 'SUSPECT.NOT_CONTROLLED.PRESCRIBED'),
('6B', 'SUSPECT.OBSERVARTION_REPORT_REDACTED.NOT_PRESCRIBED'),
('3B', 'SUSPECT.OBSERVARTION_REPORT_REDACTED.PRESCRIBED'),
('6A', 'SUSPECT.OFFICIAL_REPORT_DRAWN_UP.NOT_PRESCRIBED'),
('3B', 'SUSPECT.OFFICIAL_REPORT_DRAWN_UP.PRESCRIBED'),
('4', 'SUSPECT.PRIOR_LETTER_SENT.NOT_PRESCRIBED'),
('8', 'SUSPECT.REHABILITATED.NOT_PRESCRIBED'),
('3B', 'SUSPECT.REHABILITATED.PRESCRIBED');

CREATE TABLE analytics.workflow_transitions (
    from_step VARCHAR(10) NOT NULL,
    to_step   VARCHAR(10) NOT NULL,
    PRIMARY KEY (from_step, to_step),
    FOREIGN KEY (from_step) REFERENCES analytics.workflow_steps(step_code),
    FOREIGN KEY (to_step)   REFERENCES analytics.workflow_steps(step_code)
);

INSERT INTO analytics.workflow_transitions (from_step, to_step) VALUES
-- Importé vers statuts propagés
('0', '1'),
('0', '3B'),


-- DNV vers statut de validation qualifié
('1', '2A'),
('1', '2B'),
('1', '2C'),

-- Suspect  → non controlé ou prescrit
('2A', '3A'),
('2A', '3B'),


-- Suspect  → Controle préalable, controle terrain ou remis en état direct
('2A', '4'),
('2A', '5'),
('2A', '8'),

-- Controle préalable → controle terrain ou remis en état direct
('4', '5'),
('4', '8'),

-- controle terrain →  PV ou rapport de constatation ou remis en état direct
('5', '6A'),
('5', '6B'),
('5', '8'),

-- PV ou rapport de constatation -> astrente ou remise en état
('6A', '7'),
('6A', '8'),
('6B', '7'),
('6B', '8'),

-- Astreinte -> remise en état
('7', '8');


-- c. generer le flow metier 

CREATE MATERIALIZED VIEW analytics.flow_business_status_30169_ze_2024_v1
TABLESPACE pg_default
AS SELECT fdcs.detection_id,
    fdcs.detection_data_id,
    fdcs.old_status,
    fdcs.new_status,
    wsms_old.step_code AS source_step,
    wsms_new.step_code AS target_step,
    fdcs.history_date,
    fdcs.parcel_id,
    'app' as "process"
   FROM analytics.tracked_app_changes_30169_ze_2024_v1 fdcs
     LEFT JOIN analytics.workflow_steps_mapping_status wsms_old ON fdcs.old_status = wsms_old.status_code::text
     LEFT JOIN analytics.workflow_steps_mapping_status wsms_new ON fdcs.new_status = wsms_new.status_code::text
  WHERE wsms_old.step_code::text <> wsms_new.step_code::text
  union 
  SELECT fdcs.detection_id,
    fdcs.detection_data_id,
    fdcs.old_status,
    fdcs.new_status,
    wsms_old.step_code AS source_step,
    wsms_new.step_code AS target_step,
    fdcs.history_date,
    fdcs.parcel_id,
    'import' as "process"
   FROM analytics.tracked_import_changes_30169_ze_2024_v1 fdcs
     LEFT JOIN analytics.workflow_steps_mapping_status wsms_old ON fdcs.old_status = wsms_old.status_code::text
     LEFT JOIN analytics.workflow_steps_mapping_status wsms_new ON fdcs.new_status = wsms_new.status_code::text
  WHERE wsms_old.step_code::text <> wsms_new.step_code::text
WITH DATA;

