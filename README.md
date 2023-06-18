# Fire-Incident

### Introduction
In order to collect correctly the data I used a GCS bucket as datalake : [link](https://console.cloud.google.com/storage/browser/fire_datalake;tab=objects?project=fire-incident-dispatch-data&prefix=&forceOnObjectsSortingFiltering=false)
and a BigQuery dataset as the DWH : [link]() . 
The data pipelines is an Airflow DAG which run daily. 

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
