from datetime import datetime, timedelta

import airflow
import boto3
import base64
from airflow import DAG
from airflow.utils import db
from airflow.models import connection
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "(Data Engineering) Docker Login",
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 1,
    "concurrency": 3,
    "retry_delay": timedelta(minutes=5),
    "email": ["datasquad645@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}


def ecr_login():
    ecr = boto3.client("ecr", region_name="ap-southeast-2")
    response = ecr.get_authorization_token()

    username, password = (
        base64.b64decode(response["authorizationData"][0]["authorizationToken"])
        .decode()
        .split(":")
    )
    registry_url = response["authorizationData"][0]["proxyEndpoint"]

    with db.create_session() as session:
        docker_connection = (
            session.query(connection.Connection)
            .filter(connection.Connection.conn_id == "ecr_docker")
            .one_or_none()
        )

        if not docker_connection:
            docker_connection = connection.Connection(
                conn_id="ecr_docker",
                conn_type="docker",
                host=registry_url,
                login=username,
            )
            session.add(docker_connection)

        docker_connection.set_password(password)


with DAG(
    dag_id="de_docker_login",
    default_args=default_args,
    schedule_interval="0 */4 * * *",
    max_active_runs=1,
) as dag:
    docker_login_for_ecr = PythonOperator(
        task_id="docker_login_for_ecr", python_callable=ecr_login
    )

docker_login_for_ecr
