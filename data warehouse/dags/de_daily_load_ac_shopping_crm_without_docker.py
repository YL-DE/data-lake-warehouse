##test ci/cd pipeline in aws code pipeline
from datetime import datetime, timedelta
import pendulum
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

local_tz = pendulum.timezone("Australia/Melbourne")

default_args = {
    "owner": "(Data Engineering) Daily load from ac shopping crm",
    "depends_on_past": False,
    "start_date": datetime(2021, 2, 7, 12, 00, tzinfo=local_tz),
    "email": ["datasquad645@gmail.com",],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# List of tables to ingest
tables = ["customer", "product_upload","order","product"]

with DAG(
    "de_daily_load_ac_shopping_crm_without_docker",
    catchup=False,
    default_args=default_args,
    schedule_interval="30 0 * * *",
    max_active_runs=1,
    tags=["DE"],
) as dag:
    ingestion_tasks = {
        table: BashOperator(
            task_id=table,
            bash_command="python3 /home/airflow/airflow/apps/pipeline/data_ingestion_runner.py -c /home/airflow/airflow/apps/pipeline/projects/data_ingestion/config/ac_shopping_crm.yml -t {}".format(
                table
            ),
            dag=dag,
        )
        for table in tables
    }
# Define dependency graph
ingestion_tasks["customer"]>>ingestion_tasks["order"]
ingestion_tasks["product"]
ingestion_tasks["product_upload"]>>ingestion_tasks["order"]