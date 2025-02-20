import sys
import os
import json
import pandas
import datetime
from datetime import datetime as dt

from utils.tasks import BaseTask
from utils.postgressql_connector import PostgresSqlConnector
from utils.ssm import get_parameter_value, update_parameter_value

BASE_PATH = f"{os.getcwd()}/projects/data_processing"
CONN_SECRET_NAME = "redshift_ac_master"


class RedshiftDataProcessing(BaseTask):
    def __init__(
        self, task_name, sql_path=None, params=None, validation=None,
    ):
        self.sql_path = sql_path
        self.params = params
        self.validation = validation
        BaseTask.__init__(self, task_name)

    def args(self, parser):
        task_config = parser.add_argument_group("Task Arguments")
        task_config.add_argument(
            "-s",
            "--sql_path",
            type=str,
            help="SQL script path. e.g.dim_customer_etl ",
            required=False,
        )
        task_config.add_argument(
            "-p",
            "--params",
            type=str,
            help="""
                JSON array of parameters to use in the sql script
                (e.g. { "de_daily_load_data_warehouse_dim_customer": "None"})
            """,
            required=False,
        )
        task_config.add_argument(
            "-v",
            "--validation",
            type=int,
            help="""
                Flag to indicate whether to run any available
                data validation checks. Defaults to True (1).
            """,
            required=False,
            default=1,
        )

        args, _ = parser.parse_known_args()

    def configure(self, args):
        self.sql_path = self.sql_path or args.sql_path
        self.params = self.params or args.params

        if self.params != {}:
            self.params = json.loads(self.params)
            for key, value in self.params.items():
                if value == "None":
                    self.params[key] = get_parameter_value(key)

        self.validation = self.validation or args.validation
        self.redshift_con = PostgresSqlConnector(CONN_SECRET_NAME)

    def __execute_sql(self):
        file_path = f"{BASE_PATH}/sql/{self.sql_path}.sql"
        if os.path.exists(file_path):
            with open(file_path) as sql_file:
                sql = sql_file.read()
                sql = sql.format(**self.params) if self.params != {} else sql

            self.redshift_con.execute_sql(sql=sql)

        else:
            print(f"No sql file found at {file_path}, so execution will be skipped.")

    def __execute_data_validation_check(self):
        file_path = f"{BASE_PATH}/data_validation/{self.sql_path}.sql"
        if os.path.exists(file_path):
            with open(file_path) as sql_file:
                sql = sql_file.read()

            df = self.redshift_con.query_sql(query=sql)

            # return exception if validation check failed
            if not df.empty:
                err = f"Data Validation Check failed for {self.sql_path}:\n"
                failed_checks = ".\n".join(df.iloc[:, 0].tolist())
                raise Exception(f"{err}{failed_checks}")

    def main(self):
        # Execute Redshift script
        self.__execute_sql()

        # Execute corresponding data validation check if provided
        if self.validation == 1:
            self.__execute_data_validation_check()
        if self.params is not None:
            for parameter in self.params.items():
                update_parameter_value(parameter[0], str(dt.utcnow()))

