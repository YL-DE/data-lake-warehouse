from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
import json


class BaseFargateOperator(EcsRunTaskOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(
            task_definition=None,
            cluster="ac-shopping-analytics",
            overrides=None,
            *args,
            **kwargs,
        )
        self.region_name = "ap-southeast-2"
        self.launch_type = "FARGATE"
        self.awslogs_group = "/ecs/data-ingestion"

    def execute(self, context):
        try:
            super().execute(context)
        except Exception as e:
            raise AirflowException(f"ECS Fargate Operator error:{str(e)}")


class DataIngestionContainerOperatorFargate(BaseFargateOperator):
    ui_color = "#e9fffe"

    @apply_defaults
    def __init__(self, config_path, table_name=None, overrides=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config_path = config_path
        self.table_name = table_name
        self.task_definition = "database-ingestion"
        self.overrides = dict(
            {
                "containerOverrides": [
                    {
                        "name": "database-ingestion",
                        "command": self.construct_command()
                        # [
                        #     "python3",
                        #     "data_ingestion_runner.py",
                        #     "--config_path",
                        #     self.config_path,
                        #     "--table_name",
                        #     self.table_name,
                        # ],
                    }
                ]
            },
        )

        self.network_configuration = dict(
            {
                "awsvpcConfiguration": {
                    "subnets": ["subnet-038f4565"],
                    "assignPublicIp": "ENABLED",
                }
            }
        )

    def construct_command(self):
        cmd = ["python3", "data_ingestion_runner.py", "--config_path", self.config_path]
        if self.table_name != "" and self.table_name is not None:
            cmd.extend(["--table_name", self.table_name])

        return cmd


class RedshiftContainerOperatorFargate(BaseFargateOperator):
    ui_color = "#e9fffe"

    @apply_defaults
    def __init__(
        self,
        sql_path,
        sql_params,
        validation="1",
        overrides=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.sql_path = sql_path
        self.validation = validation
        self.sql_params = sql_params
        self.task_definition = "data-processing"
        self.overrides = dict(
            {
                "containerOverrides": [
                    {"name": "data-processing", "command": self.construct_command()}
                ]
            },
        )

        self.network_configuration = dict(
            {
                "awsvpcConfiguration": {
                    "subnets": ["subnet-038f4565"],
                    "assignPublicIp": "ENABLED",
                }
            }
        )

        ##self.awslogs_stream_prefix = "airflow-database-ingestion"

    def construct_command(self):
        cmd = [
            "python3",
            "data_processing_runner.py",
            "--sql_path",
            self.sql_path,
            "--params",
            json.dumps(self.sql_params),
            "--validation",
            self.validation,
        ]

        return cmd


class DataEngineeringOperatorFargate(BaseFargateOperator):
    ui_color = "#e9fffe"

    @apply_defaults
    def __init__(self, python_file_path, config_path, overrides=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.python_file_path = python_file_path
        self.config_path = config_path
        self.task_definition = "data-engineer-task"
        self.overrides = dict(
            {
                "containerOverrides": [
                    {
                        "name": "data-engineer-task",
                        "command": self.construct_command()
                        # [
                        #     "python3",
                        #     "data_ingestion_runner.py",
                        #     "--config_path",
                        #     self.config_path,
                        #     "--table_name",
                        #     self.table_name,
                        # ],
                    }
                ]
            },
        )

        self.network_configuration = dict(
            {
                "awsvpcConfiguration": {
                    "subnets": ["subnet-038f4565"],
                    "assignPublicIp": "ENABLED",
                }
            }
        )

        ##self.awslogs_stream_prefix = "airflow-database-ingestion"

    def construct_command(self):
        cmd = ["python", f"{self.python_file_path}", "--config_path", self.config_path]

        return cmd


class DockerOperatorPlugin(AirflowPlugin):
    name = "docker_operator_plugin"
    operators = [
        DataIngestionContainerOperatorFargate,
        RedshiftContainerOperatorFargate,
    ]
