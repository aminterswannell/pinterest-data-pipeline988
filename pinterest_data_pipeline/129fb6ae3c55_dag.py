from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 


#Define parameters for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/alexminter98@gmail.com/databricks_tasks/pinterest_data_pipeline/databricks_tasks/mount_s3_to_db',
}


#Define parameters for Run Now Operator
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': 'alex minter-swannell',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


with DAG('129fb6ae3c55_dag',
    start_date=datetime(2024, 12, 13),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run
