
dim_Alarm_schema = [{'name': 'alarm_id', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
                       {'name': 'alarm_box_location', 'type': 'STRING', 'mode': 'NULLABLE'},
                       {'name': 'alarm_box_number', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
                       {'name': 'alarm_box_borough', 'type': 'STRING', 'mode': 'REQUIRED'},
                       {'name': 'alarm_source_description_tx', 'type': 'STRING', 'mode': 'REQUIRED'},
                       {'name': 'alarm_level_index_description', 'type': 'STRING', 'mode': 'REQUIRED'},
                       {'name': 'highest_alarm_level', 'type': 'STRING', 'mode': 'REQUIRED'}]

dim_Event_schema = [{'name': 'event_id', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
                       {'name': 'first_assignment_datetime', 'type': 'DATETIME', 'mode': 'NULLABLE'},
                       {'name': 'first_activation_datetime', 'type': 'DATETIME', 'mode': 'NULLABLE'},
                       {'name': 'first_on_scene_datetime', 'type': 'DATETIME', 'mode': 'NULLABLE'},
                       {'name': 'incident_close_datetime', 'type': 'DATETIME', 'mode': 'REQUIRED'},
                       {'name': 'dispatch_response_seconds_qy', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
                       {'name': 'incident_response_seconds_qy', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
                       {'name': 'incident_travel_tm_seconds_qy', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
                       {'name': 'valid_dispatch_rspns_time_indc', 'type': 'STRING', 'mode': 'REQUIRED'},
                       {'name': 'valid_incident_rspns_time_indc', 'type': 'STRING', 'mode': 'REQUIRED'},
                       {'name': 'engines_assigned_quantity', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
                       {'name': 'ladders_assigned_quantity', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
                       {'name': 'other_units_assigned_quantity', 'type': 'NUMERIC', 'mode': 'REQUIRED'}]

dim_Authorities_schema = [{'name': 'authorities_id', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
                       {'name': 'policeprecinct', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
                       {'name': 'incident_borough', 'type': 'STRING', 'mode': 'REQUIRED'},
                       {'name': 'citycouncildistrict', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
                       {'name': 'communitydistrict', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
                       {'name': 'communityschooldistrict', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
                       {'name': 'congressionaldistrict', 'type': 'NUMERIC', 'mode': 'NULLABLE'}]

fact_Incident_schema = [{'name': 'starfire_incident_id', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
                       {'name': 'incident_datetime', 'type': 'DATETIME', 'mode': 'REQUIRED'},
                       {'name': 'incident_close_datetime', 'type': 'DATETIME', 'mode': 'REQUIRED'},
                       {'name': 'alarm_id', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
                       {'name': 'event_id', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
                       {'name': 'authorities_id', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
                       {'name': 'alarm_box_location', 'type': 'STRING', 'mode': 'NULLABLE'},
                       {'name': 'incident_borough', 'type': 'STRING', 'mode': 'REQUIRED'},
                       {'name': 'zipcode', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
                       {'name': 'incident_classification', 'type': 'STRING', 'mode': 'REQUIRED'},
                       {'name': 'incident_classification_group', 'type': 'STRING', 'mode': 'REQUIRED'}]
