# Fire-Incident

### Introduction
In order to collect correctly the data I used a GCS bucket as datalake [(datalake link)](https://console.cloud.google.com/storage/browser/fire_datalake;tab=objects?project=fire-incident-dispatch-data&prefix=&forceOnObjectsSortingFiltering=false)
and a BigQuery dataset as the DWH [(DWH link)](console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sfire-incident-dispatch-data!2stest) . 
The data pipelines is an Airflow DAG (named test_dag) which run daily [(web server link)](https://8cfc3e452d5c4f1186aec3a84e2e09a6-dot-us-central1.composer.googleusercontent.com/home). 

### Environment 
As mentioned, I used an Airflow Dag running on Google composer.
The tag toggle is turned off (please try to avoid turning it on). 
I gave permission to the team's email for datalake, DWH, Airflow webserver (check email invitation ).

## ETL description

![image](https://github.com/samuelharroch/Fire-Incident/assets/60176709/dcc97641-8ec4-4f5a-ab6a-fee46ffe2c9d)

### fetch_and_save_data task 
First we are collecting the data from the source API using pagination (limit and offset) and then store the response (data)
into multiple json files (according to the pagination) in the GCS bucket .
(stage 1)

### transform task 
Then, loading the data (multiple json files) from the datalake and performing transformation (see plugins.tranform_plugin.py)
by seperating to table (according to DWH dataset) and adding "relation keys" (kind of foreign keys). 
Then save the results into (multiple) CSV files in the datalake. 
(stage 2)

### delete_tables_rows tasks
As we want to overwrite the data, before inserting new data in the DWH dataset we need to delete the rows from the tables.
It's probably not the best practice but It was the best I could do at this moment. 
(The best practice can be to overwrite correctly by primary keys or other similar method)

### insert_tables_to_bigquery tasks
Then we are loading the data from CSV files to BigQuery tables directly (with the help of BigQueryHook.run_load )

## Deliverables

1. Code in dags (main code) and plugins folder (helper functions..) 
2. SQL queries - in SQL folder, including create tables query and demonstration queries.
3. Data model document  [(Data model link)](https://github.com/samuelharroch/Fire-Incident/blob/main/SQL/date_model.png) - The data modeling is based on Kimball dimensional modelling, according to the assignment requirements. Personally, as we are using bigquery for DWH, I think it was preferable to model the data into 1 dimension (1 table)...
4. Description of the ETL pipeline - this README.md file