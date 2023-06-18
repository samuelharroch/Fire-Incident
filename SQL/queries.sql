-- incident_classification ranking

select
incident_classification,
count(*) as fire_incident_classification_count

FROM `fire-incident-dispatch-data.test.fact_Incident` as incident

GROUP BY 1
order by 2 DESC;

-- busiest borough ranking

select
incident_borough,
count(*) as fire_incident_count

FROM `fire-incident-dispatch-data.test.fact_Incident` as incident

GROUP BY 1
order by 2 DESC;


-- average assignment time in seconds,
-- can apply same query for 'first_activation_datetime', 'first_on_scene_datetime', 'incident_close_datetime'
--
select 	avg(DATETIME_DIFF(first_assignment_datetime, incident_datetime, SECOND)) as avg_assignment_time_sec

FROM `fire-incident-dispatch-data.test.fact_Incident` as incident

join `fire-incident-dispatch-data.test.dim_Event` as event
on  incident.event_id = event.event_id;


-- ranking avg incident close time by borough
select incident_borough	, avg(DATETIME_DIFF(incident_close_datetime, incident_datetime, SECOND)) as avg_close_sec

FROM `fire-incident-dispatch-data.test.fact_Incident` as incident

group by 1
order by 2;


-- is the avg  dispatch_response_seconds_qy in Alarm table equals to the dispatch_response_seconds_qy computed manually ?
--  (using first_assignment_datetime and incident_datetime diff )
select 	round(avg(DATETIME_DIFF(first_assignment_datetime, incident_datetime, SECOND))) = round(avg(dispatch_response_seconds_qy)) as avg_assignment_time_sec2

FROM `fire-incident-dispatch-data.test.fact_Incident` as incident

join `fire-incident-dispatch-data.test.dim_Event` as event
on  incident.event_id = event.event_id;