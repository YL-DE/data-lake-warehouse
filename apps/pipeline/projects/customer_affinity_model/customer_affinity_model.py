import sys
import os
import yaml
from utils.tasks import BaseTask
from utils.s3_connector import S3Connector
from utils.emr_connector import EMRConnector
import datetime
from datetime import datetime as dt


class CustomerAffinityModel(BaseTask):
    def __init__(
        self, task_name, config_path=None,
    ):
        self.config_path = config_path
        self.s3_conn = S3Connector()
        self.region_name = "ap-southeast-2"
        BaseTask.__init__(self, task_name)

    def args(self, parser):
        task_config = parser.add_argument_group("Task Arguments")
        task_config.add_argument(
            "-c",
            "--config_path",
            type=str,
            help="YAML config file path.",
            required=True,
        )

        args, _ = parser.parse_known_args()
        self.s3_conn.configure()

    def configure(self, args):
        self.config_path = self.config_path or args.config_path
        self.emr_cluster_config = self._get_yaml()
        self.emr_connector = EMRConnector(self.region_name, self.emr_cluster_config)

    def _get_yaml(self):
        with open(self.config_path + ".yml") as config_file:
            return yaml.full_load(config_file)

    def sync_emr_scripts_to_s3(self):
        self.s3_conn.upload_file(
            "projects/customer_affinity_model/config/bootstrap.sh",
            "ac-shopping-emr-scripts",
            "customer_affinity_model/bootstrap.sh",
        )
        self.s3_conn.upload_file(
            "projects/customer_affinity_model/etl_scripts/calculate_customer_affinity_score.py",
            "ac-shopping-emr-scripts",
            "customer_affinity_model/calculate_customer_affinity_score.py",
        )

    def main(self):

        self.sync_emr_scripts_to_s3()

        cluster_response = self.emr_connector.run_emr_cluster()

        print(cluster_response)
        step_response = self.emr_connector.add_steps_to_emr_cluster(
            job_flow_id=cluster_response["JobFlowId"]
        )

        print(step_response)
        # print(step_response["StepIds"])
        for step_id in step_response["StepIds"]:
            self.emr_connector.watch_emr_step_status(
                job_flow_id=cluster_response["JobFlowId"], step_id=step_id,
            )

        if self.emr_connector.get_emr_cluster_status(cluster_response["JobFlowId"]) in [
            "RUNNING",
            "WAITING",
        ]:
            self.emr_connector.terminate_running_emr_cluster(
                cluster_response["JobFlowId"]
            )
