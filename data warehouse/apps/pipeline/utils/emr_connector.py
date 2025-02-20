import sys
import os
import boto3
from botocore.exceptions import ClientError

from datetime import datetime as dt
import time


class EMRConnector:
    def __init__(self, region_name, emr_cluster_config):
        self.region_name = region_name or "ap-southeast-2"
        self.emr_cluster_config = emr_cluster_config
        self.emr_client = boto3.client("emr", region_name=region_name)

    def run_emr_cluster(self):

        try:
            response = self.emr_client.run_job_flow(
                Name=self.emr_cluster_config["Name"],
                ReleaseLabel=self.emr_cluster_config["ReleaseLabel"],
                Instances=self.emr_cluster_config["Instances"],
                BootstrapActions=self.emr_cluster_config["BootstrapActions"],
                Applications=self.emr_cluster_config["Applications"],
                Configurations=self.emr_cluster_config["Configurations"],
                VisibleToAllUsers=self.emr_cluster_config["VisibleToAllUsers"],
                JobFlowRole=self.emr_cluster_config["JobFlowRole"],
                ServiceRole=self.emr_cluster_config["ServiceRole"],
                LogUri=self.emr_cluster_config["LogUri"],
            )

            return response
        except Exception as e:
            print("Cluster creation failed {}".format(e))
            raise e

    def get_emr_cluster_status(self, job_flow_id):
        try:
            response = self.emr_client.describe_cluster(ClusterId=job_flow_id)
            cluster_status = response["Cluster"]["Status"]["State"]
            return cluster_status
        except Exception as e:
            print(e)
            raise e

    def add_steps_to_emr_cluster(self, job_flow_id):
        print("Add emr script as step to created emr cluster")
        try:
            steps = self.emr_cluster_config["Steps"]
            response = self.emr_client.add_job_flow_steps(
                JobFlowId=job_flow_id, Steps=steps
            )

            return response

        except Exception as e:
            self.emr_client.terminate_job_flows(JobFlowIds=["{}".format(job_flow_id)])
            print(
                "Cluser {} has been terminated due to job exception".format(job_flow_id)
            )
            raise e

    def watch_emr_step_status(self, job_flow_id, step_id, max_running_hours=None):
        print(
            "Watch added emr step status, step {} started at {}".format(
                step_id, dt.now()
            )
        )
        step_state = ""
        timer = 0
        if max_running_hours is None:
            timer_max = 14400
        else:
            timer_max = max_running_hours * 3600
        # set timer to allow maximum running time of 4 hours for each emr step if not specified
        try:
            while (
                step_state
                not in [
                    "COMPLETED",
                    "CANCELLED",
                    "FAILED",
                    "INTERRUPTED",
                    "CANCEL_PENDING",
                ]
                and timer <= timer_max
            ):
                print("Step {} is pending or runnning at {}".format(step_id, dt.now()))
                time.sleep(120)
                step_state = self.emr_client.describe_step(
                    ClusterId=job_flow_id, StepId=step_id
                )["Step"]["Status"]["State"]
                if step_state == "RUNNING":
                    timer = timer + 120
            else:
                if step_state == "COMPLETED":
                    print(
                        "Step {} has completed successfully at {}".format(
                            step_id, dt.now()
                        )
                    )
                elif step_state in ["CANCELLED", "CANCEL_PENDING", "INTERRUPTED"]:
                    print(
                        "Step {} has been cancelled/interrupted at {}".format(
                            step_id, dt.now()
                        )
                    )
                elif step_state == "FAILED":
                    print("Step {} failed at {}".format(step_id, dt.now()))
                elif timer > timer_max:
                    print(
                        "Step {} running passed maximum allowed processing duration, cluster will be terminated".format(
                            step_id
                        )
                    )
                    self.emr_client.terminate_job_flows(
                        JobFlowIds=["{}".format(job_flow_id)]
                    )

        except Exception as e:
            self.emr_client.terminate_job_flows(JobFlowIds=["{}".format(job_flow_id)])
            print(
                "Cluser {} has been terminated due to job exception".format(job_flow_id)
            )
            raise e

    def terminate_running_emr_cluster(self, job_flow_id):
        try:
            response = self.emr_client.terminate_job_flows(
                JobFlowIds=["{}".format(job_flow_id)]
            )
            print(
                "Cluser {} has been terminated due to user request after job finished".format(
                    job_flow_id
                )
            )

        except Exception as e:
            print(e)
            raise e
