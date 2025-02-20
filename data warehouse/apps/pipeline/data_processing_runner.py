# from ingest_data import  DataIngestionTask
from projects.data_processing.data_processing import RedshiftDataProcessing


def execute():
    task = RedshiftDataProcessing("RedshiftDataProcessingTask")
    task.run()


if __name__ == "__main__":
    execute()
