
-- "need to add indexing, partitioning and maybe clustering "

create table if not exists `fire-incident-dispatch-data.fire_incident_dispatch_data.dim_Alarm`
(
  alarm_id NUMERIC PRIMARY KEY not enforced NOT NULL,
  alarm_box_location STRING,
  alarm_box_number NUMERIC NOT NULL,
  alarm_box_borough STRING NOT NULL,
  alarm_source_description_tx STRING NOT NULL,
  alarm_level_index_description STRING NOT NULL,
  highest_alarm_level STRING NOT NULL
)
CLUSTER BY alarm_box_borough , alarm_box_location;

create table if not exists `fire-incident-dispatch-data.fire_incident_dispatch_data.dim_Event`
(
  event_id NUMERIC PRIMARY KEY not enforced NOT NULL,
  first_assignment_datetime DATETIME ,
  first_activation_datetime DATETIME ,
  first_on_scene_datetime DATETIME ,
  incident_close_datetime DATETIME NOT NULL,
  dispatch_response_seconds_qy NUMERIC NOT NULL,
  incident_response_seconds_qy NUMERIC NOT NULL,
  incident_travel_tm_seconds_qy NUMERIC NOT NULL,
  valid_dispatch_rspns_time_indc STRING NOT NULL,
  valid_incident_rspns_time_indc STRING NOT NULL,
  engines_assigned_quantity NUMERIC NOT NULL,
  ladders_assigned_quantity NUMERIC NOT NULL,
  other_units_assigned_quantity NUMERIC NOT NULL

)
PARTITION BY
  DATE(first_assignment_datetime)
   CLUSTER BY first_activation_datetime , first_on_scene_datetime ;

create table if not exists `fire-incident-dispatch-data.fire_incident_dispatch_data.dim_Authorities`
(
  authorities_id NUMERIC NOT NULL PRIMARY KEY not enforced,
  policeprecinct NUMERIC ,
  incident_borough STRING NOT NULL,
  citycouncildistrict NUMERIC ,
  communitydistrict NUMERIC ,
  communityschooldistrict NUMERIC ,
  congressionaldistrict NUMERIC

)
CLUSTER BY incident_borough , policeprecinct ;

create table if not exists `fire-incident-dispatch-data.fire_incident_dispatch_data.fact_Incident`
(
  starfire_incident_id NUMERIC ,
  incident_datetime DATETIME NOT NULL,
  incident_close_datetime DATETIME NOT NULL,
  alarm_id NUMERIC NOT NULL,
  event_id NUMERIC NOT NULL,
  authorities_id NUMERIC NOT NULL,
  alarm_box_location STRING ,
  incident_borough STRING NOT NULL,
  zipcode NUMERIC,
  incident_classification STRING NOT NULL,
  incident_classification_group STRING NOT NULL,

  FOREIGN KEY (alarm_id) REFERENCES `fire-incident-dispatch-data.fire_incident_dispatch_data.dim_Alarm` (alarm_id) NOT ENFORCED,
  FOREIGN KEY (event_id) REFERENCES `fire-incident-dispatch-data.fire_incident_dispatch_data.dim_Event` (event_id) NOT ENFORCED,
  FOREIGN KEY (authorities_id) REFERENCES `fire-incident-dispatch-data.fire_incident_dispatch_data.dim_Authorities` (authorities_id) NOT ENFORCED


)
PARTITION BY
  DATE(incident_datetime)
   CLUSTER BY incident_borough, alarm_box_location ;

