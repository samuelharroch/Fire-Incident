from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from tranform_plugin import transform_data
from schema_plugin import dim_Alarm_schema, dim_Event_schema, dim_Authorities_schema, fact_Incident_schema

from datetime import datetime
import requests
from google.cloud import storage
import logging
import pandas as pd
import json
import time


default_args = {
    'start_date': datetime(2023, 6, 1),
}


def fetch_and_save_data_to_gcs(ti):
    url = "https://data.cityofnewyork.us/resource/8m42-w767.json"
    params = {
        "$limit": 50000,  # Number of records per page
        "$offset": 0,  # Initial offset
    }

    gcs_bucket_name = 'fire_datalake'
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)

    while True:
        # Send a GET request to the API with pagination parameters
        logging.info(f"offset: {params['$offset']}")
        response = requests.get(url, params=params, headers={'X-App-Token': 'qXe3zuKKhGzqrYSw9Jgmys176'})

        if response.status_code == 200:

            data_json = response.json()

            # Save data to GCS

            gcs_file_name = 'data' + str(params["$offset"]) + '.json'

            blob = bucket.blob(gcs_file_name)

            blob.upload_from_string(data=json.dumps(data_json), content_type='application/json')
            logging.info(f"Data saved to GCS bucket: gs://{gcs_bucket_name}/{gcs_file_name}")

            # Increment the offset for the next page
            params["$offset"] += params["$limit"]

            # Check if there are more records available
            if len(data_json) < params["$limit"]:
                break  # Exit the loop if no more records

        else:
            logging.warning(f"Error: {response.status_code} - {response.reason}")

    ti.xcom_push(key='last_offset', value=params['$offset'])


def _transform(ti):
    last_offset = ti.xcom_pull(key='last_offset', task_ids='fetch_and_save_data')

    for offset in range(0, last_offset, 50000):

        # Retrieve data from GCS
        gcs_bucket_name = 'gs://fire_datalake'
        gcs_file_name = 'data' + str(offset) + '.json'
        gcs_path = '/'.join([gcs_bucket_name, gcs_file_name])

        # Load data into Pandas DataFrame
        df = pd.read_json(gcs_path)

        # Apply transformation
        incident_df, alarm_df, event_df, authorities_df = transform_data(df, offset)

        # Save transformed data to GCS
        transformed_gcs_bucket_name = 'fire_datalake'

        # incident_df
        transformed_gcs_file_name = gcs_bucket_name + '/' + 'transformed_incident' + str(offset) + '.csv'
        incident_df.to_csv(transformed_gcs_file_name, index=False)
        logging.info(f"Transformed data saved to GCS bucket: gs://{transformed_gcs_bucket_name}/{transformed_gcs_file_name}")

        time.sleep(3)

        # alarm_df
        transformed_gcs_file_name = gcs_bucket_name + '/' + 'transformed_alarm' + str(offset) + '.csv'
        alarm_df.to_csv(transformed_gcs_file_name, index=False)
        logging.info(
            f"Transformed data saved to GCS bucket: gs://{transformed_gcs_bucket_name}/{transformed_gcs_file_name}")

        time.sleep(3)

        # event_df
        transformed_gcs_file_name = gcs_bucket_name + '/' + 'transformed_event' + str(offset) + '.csv'
        event_df.to_csv(transformed_gcs_file_name, index=False)

        logging.info(
            f"Transformed data saved to GCS bucket: gs://{transformed_gcs_bucket_name}/{transformed_gcs_file_name}")

        time.sleep(3)

        # authorities_df
        transformed_gcs_file_name = gcs_bucket_name + '/' + 'transformed_authorities' + str(offset) + '.csv'
        authorities_df.to_csv(transformed_gcs_file_name, index=False)
        logging.info(
            f"Transformed data saved to GCS bucket: gs://{transformed_gcs_bucket_name}/{transformed_gcs_file_name}")

        # Sleep for 3 second
        time.sleep(3)


def _insert_table_to_bigquery(ti, table_id, gcs_file_name_prefix,schema_fields):

    last_offset = ti.xcom_pull(key='last_offset', task_ids='fetch_and_save_data')
    # Create BigQuery hook
    bq_hook = BigQueryHook()

    gcs_bucket_name = 'fire_datalake'

    # BigQuery details
    project_id = 'fire-incident-dispatch-data'
    dataset_id = 'test'

    for offset in range(0, last_offset, 50000):

        # GCS file details
        gcs_file_name = gcs_file_name_prefix + str(offset) + '.csv'

        # Get GCS file URI
        gcs_uri = f'gs://{gcs_bucket_name}/{gcs_file_name}'

        # Load data from CSV file to BigQuery table
        bq_hook.run_load(
            destination_project_dataset_table='.'.join([project_id, dataset_id, table_id]),
            source_format='CSV',
            source_uris=[gcs_uri],
            skip_leading_rows=1,
            field_delimiter=',',
            schema_fields=schema_fields,
            write_disposition='WRITE_APPEND'
        )

        # Sleep for 2 second
        time.sleep(2)


with DAG('test_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    fetch_and_save_data_task = PythonOperator(
        task_id='fetch_and_save_data',
        python_callable=fetch_and_save_data_to_gcs
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=_transform
    )

    delete_alarm_rows = BigQueryExecuteQueryOperator(
        task_id='delete_alarm_rows',
        sql='DELETE FROM `fire-incident-dispatch-data.test.dim_Alarm` where 1=1',
        use_legacy_sql=False,
    )

    delete_event_rows = BigQueryExecuteQueryOperator(
        task_id='delete_event_rows',
        sql='DELETE FROM `fire-incident-dispatch-data.test.dim_Event` where 1=1',
        use_legacy_sql=False,
    )

    delete_authorities_rows = BigQueryExecuteQueryOperator(
        task_id='delete_authorities_rows',
        sql='DELETE FROM `fire-incident-dispatch-data.test.dim_Authorities` where 1=1',
        use_legacy_sql=False,
    )

    delete_incident_rows = BigQueryExecuteQueryOperator(
        task_id='delete_incident_rows',
        sql='DELETE FROM `fire-incident-dispatch-data.test.fact_Incident` where 1=1',
        use_legacy_sql=False,
    )

    insert_alarm_to_bigquery = PythonOperator(
        task_id='insert_alarm_to_bigquery',
        python_callable=_insert_table_to_bigquery,
        op_kwargs={'table_id': 'dim_Alarm',
                   'gcs_file_name_prefix': 'transformed_alarm',
                   'schema_fields': dim_Alarm_schema}
    )

    insert_event_to_bigquery = PythonOperator(
        task_id='insert_event_to_bigquery',
        python_callable=_insert_table_to_bigquery,
        op_kwargs={'table_id': 'dim_Event',
                   'gcs_file_name_prefix': 'transformed_event',
                   'schema_fields':dim_Event_schema }
    )

    insert_authorities_to_bigquery = PythonOperator(
        task_id='insert_authorities_to_bigquery',
        python_callable=_insert_table_to_bigquery,
        op_kwargs={'table_id': 'dim_Authorities',
                   'gcs_file_name_prefix': 'transformed_authorities',
                   'schema_fields':dim_Authorities_schema}
    )

    insert_incident_to_bigquery = PythonOperator(
        task_id='insert_incident_to_bigquery',
        python_callable=_insert_table_to_bigquery,
        op_kwargs={'table_id': 'fact_Incident',
                   'gcs_file_name_prefix': 'transformed_incident',
                   'schema_fields': fact_Incident_schema}
    )


    fetch_and_save_data_task >> transform >> [delete_alarm_rows ,delete_event_rows , delete_authorities_rows,delete_incident_rows]
    delete_alarm_rows >> insert_alarm_to_bigquery
    delete_event_rows >> insert_event_to_bigquery
    delete_authorities_rows >> insert_authorities_to_bigquery
    delete_incident_rows >> insert_incident_to_bigquery
