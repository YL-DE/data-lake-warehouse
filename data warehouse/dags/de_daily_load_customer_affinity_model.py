from datetime import datetime, timedelta
import pendulum
import airflow
from airflow import DAG
from docker_operator_plugin import (
    DataEngineeringOperatorFargate,
    RedshiftContainerOperatorFargate,
)

local_tz = pendulum.timezone("Australia/Melbourne")
# fix bug

default_args = {
    "owner": "(Data Engineering) Daily load to process customer affinity model",
    "depends_on_past": False,
    "start_date": datetime(2021, 2, 7, 12, 00, tzinfo=local_tz),
    "email": ["datasquad645@gmail.com",],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "de_daily_load_customer_affinity_model",
    catchup=False,
    default_args=default_args,
    schedule_interval="30 6 * * *",
    max_active_runs=1,
    tags=["DE"],
) as dag:

    redshift_export_task = RedshiftContainerOperatorFargate(
        task_id="redshfit_export_data",
        sql_path="export_affinity_model",
        sql_params={},
        dag=dag,
    )
    calculate_customer_affinity_score_task = DataEngineeringOperatorFargate(
        task_id="calculate_customer_affinity_model",
        python_file_path="customer_affinity_model_runner.py",
        config_path="projects/customer_affinity_model/config/customer_affinity_model",
        dag=dag,
    )
# Define dependency graph
redshift_export_task >> calculate_customer_affinity_score_task
