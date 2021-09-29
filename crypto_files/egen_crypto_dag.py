import pandas as pd
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from google.cloud import storage
from google.cloud import bigquery

DAG_NAME = "egen_crypto_dag"

default_args = {
    "depends_on_past": False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2017, 11, 1)
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
    description=DAG_NAME,
    max_active_runs=5,
)


def get_ensemble_records():
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('egen_crypto_dataset')
    blobs = bucket.list_blobs()
    combined_df = pd.DataFrame()

    # downloading all csv files from the google cloud storage bucket and combining it into one dataframe
    for blob in blobs:
        filename = blob.name.replace('/', '_')
        print(f"Downloading file {filename}")

        blob.download_to_filename(f'/home/airflow/gcs/data/capstone/{filename}')
        print(f"Concatenating {filename} together into a single dataframe")
        read_file_df = pd.read_csv(f'/home/airflow/gcs/data/capstone/{filename}')
        combined_df = combined_df.append(read_file_df)

        # deleting the csv file from google cloud storage bucket
        print(f"Deleting file {filename}")
        blob.delete()

    # return combined_df as csv
    if len(combined_df) > 0:
        ensembled_file_name = f"combined_files.csv"
        combined_df.to_csv(f"/home/airflow/gcs/data/capstone/{ensembled_file_name}", index=False)
        print("Found files moving ahead to further tasks")
        return "clean_and_process_records"
    else:
        print("N/A files found, ending this run")
        return "completed"


def clean_and_process_records():
    read_ensembled_df = pd.read_csv('/home/airflow/gcs/data/capstone/combined_files.csv')
    read_ensembled_df = read_ensembled_df.fillna("").astype(str)

    crypto_symbols = ['BTC', 'ETH', 'LTC', 'DOT', 'DOGE', 'XLM', 'ETC', 'LINK', 'ATOM', 'ALGO']
    temp = pd.DataFrame()
    print(read_ensembled_df.columns)
    for i in crypto_symbols:
        print(i)
        temp = temp.append(read_ensembled_df[read_ensembled_df["id"] == i], ignore_index=True)

    temp.sort_values("price_timestamp", inplace=True)
    new_ensembled_df = temp[['id', 'currency', 'symbol', 'name', 'price', 'price_timestamp', 'circulating_supply', 'rank']].copy()

    new_ensembled_df.columns = ['id', 'currency', 'symbol', 'name', 'price', 'price_timestamp', 'circulating_supply', 'rank']
    new_ensembled_df.to_csv('/home/airflow/gcs/data/weather_data/clean_records.csv', index=False)


def upload_to_bigquery():
    client = bigquery.Client()

    table_id = 'egendemo.crypto_price.crypto_table'
    destination_table = client.get_table(table_id)

    row_count_before_inserting = destination_table.num_rows
    print(f"rows before insert: {row_count_before_inserting}")

    if row_count_before_inserting > 0:
        disposition = bigquery.WriteDisposition.WRITE_APPEND
        print(f"rows before insert: {row_count_before_inserting} i.e > 0 so disposition is {disposition}")
    elif row_count_before_inserting == 0:
        disposition = bigquery.WriteDisposition.WRITE_EMPTY
        print(f"rows before insert: {row_count_before_inserting} i.e = 0 so disposition is {disposition}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=disposition,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    uri = f'gs://us-east1-egenairflow2-cbdd2a40-bucket/data/capstone/clean_records.csv'
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    load_job.result()

    destination_table = client.get_table(table_id)
    rows_after_insert = destination_table.num_rows
    print(f"rows after insert: {rows_after_insert}")
    print(
        f"Inserted {abs(row_count_before_inserting - rows_after_insert)} in the table. Total {rows_after_insert} rows in table now.")


started = DummyOperator(
    task_id="started",
    dag=dag)

ensemble_records = BranchPythonOperator(
    task_id='ensemble_records',
    python_callable=get_ensemble_records,
    dag=dag
)

clean_and_process_records = PythonOperator(
    task_id='clean_and_process_records',
    python_callable=clean_and_process_records,
    dag=dag
)

upload_records_to_bigquery = PythonOperator(
    task_id='upload_records_to_bigquery',
    python_callable=upload_to_bigquery,
    dag=dag
)

completed = DummyOperator(
    task_id="completed",
    dag=dag)

started >> ensemble_records >> clean_and_process_records >> upload_records_to_bigquery >> completed
ensemble_records >> completed
