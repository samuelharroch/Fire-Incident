import pandas as pd


def table_alarm(df):
    alarm_df = df[['alarm_id', 'alarm_box_location', 'alarm_box_number',
                   'alarm_box_borough', 'alarm_source_description_tx',
                   'alarm_level_index_description', 'highest_alarm_level']].copy()
    return alarm_df


def table_event(df):
    event_df = df[['event_id', 'first_assignment_datetime', 'first_activation_datetime', 'first_on_scene_datetime',
                   'incident_close_datetime', 'dispatch_response_seconds_qy', 'incident_response_seconds_qy',
                   'incident_travel_tm_seconds_qy', 'valid_dispatch_rspns_time_indc', 'valid_incident_rspns_time_indc',
                   'engines_assigned_quantity',
                   'ladders_assigned_quantity', 'other_units_assigned_quantity']].copy()
    return event_df


def table_authorities(df):
    authorities_df = df[['authorities_id', 'policeprecinct', 'incident_borough', 'citycouncildistrict',
                         'communitydistrict', 'communityschooldistrict', 'congressionaldistrict']].copy()
    return authorities_df


def table_incident(df):
    incident_df = df[['starfire_incident_id', 'incident_datetime', 'incident_close_datetime',
                      'alarm_id', 'event_id', 'authorities_id', 'alarm_box_location', 'incident_borough', 'zipcode',
                      'incident_classification', 'incident_classification_group']].copy()
    return incident_df


def transform_data(df, offset):

    df['alarm_id'] = range(offset, offset+len(df))
    df['event_id'] = range(offset, offset+len(df))
    df['authorities_id'] = range(offset, offset+len(df))

    incident_df = table_incident(df)

    authorities_df = table_authorities(df)

    event_df = table_event(df)

    alarm_df = table_alarm(df)

    return incident_df, alarm_df, event_df, authorities_df
