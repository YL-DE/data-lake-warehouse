import datetime as dt
import time
import uuid
import argparse


class BaseTask:
    """
    Base Task class to make use of framework
    """

    def __init__(self, task_name):
        self.task_start = time.time()
        self.task_name = task_name.lower().replace(" ", "_")
        self.task_id = str(uuid.uuid4())
        self.task_status = "Ready"
        self._initialize()

    def _initialize(self):
        try:
            self.task_status = "Initialize"
            parser = argparse.ArgumentParser(description="Process Arguments for Tasks")
            self.args(parser)
            args = parser.parse_args()
            self.configure(args)
        except Exception as e:
            raise e

    def args(self, parser):
        pass

    def configure(self, args):
        pass

    def main(self):
        pass

    def run(self):
        try:
            self.task_status = "In Progress"
            self.main()
            self.task_status = "Succeeded"
        except Exception as e:
            self.task_status = "Failed"
            raise e
