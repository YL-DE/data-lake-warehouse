from asyncore import file_dispatcher
import great_expectations as ge
import pandas as pd
from sqlalchemy import column, false, true

validation_config = {
    "total_columns_count": 17,
    "columns_datatype": {
        "customer_id": "int64",
        # set this column datatype to be object instead of datetime as read_csv can only contain strings, integers and floats.
        "first_name": "object",
        "last_name": "object",
    },
    "columns_value_set": {"gender": ["f", "dfdfd"], "dob": []},
    # always set values in lower case
}


class FileValidation:
    def __init__(self, file_path, file_format, validation_config) -> None:
        # self.validation_name = validation_name
        self.validation_config = validation_config
        self.file_path = file_path
        self.file_format = file_format
        self.read_file()

    def read_file(self) -> pd.DataFrame:
        try:
            if self.file_format == "csv":
                df = pd.read_csv(self.file_path)
            elif self.file_format == "parquet":
                df = pd.read_parquet(self.file_path)
            elif self.file_format == "jsonl":
                df = pd.read_json(self.file_path, lines=True)
            self.df = df
        except Exception as ex:
            print(f"Read files into pandas df failed: {ex}")
            raise ValueError

    def validate_columns_count(self) -> bool:
        if len(self.df.columns) == self.validation_config.get("total_columns_count"):
            print(
                f"""
                File {self.file_path} Validation: total columns count check passed.
                """
            )
            return True
        else:

            print(
                f"""
                File {self.file_path} Validation: total columns count check failed.
                    - Expected {self.validation_config.get("total_columns_count")} fields, but the file has {len(self.df.columns)} fields.
            """
            )
        return False

    def validate_column_data_type(self) -> bool:
        ge_df = ge.from_pandas(self.df)
        flag = true
        for column_datatype in self.validation_config.get("columns_datatype").items():
            output = ge_df.expect_column_values_to_be_of_type(
                column_datatype[0], type_=column_datatype[1]
            )
            if output.success is False:
                flag = false
                print(
                    f"""File column datatype failed: column name is {column_datatype[0]}"""
                )

        if flag == true:
            print(
                f"""
                File '{self.file_path}' Validation: columns datatype check passed.
                """
            )
            return True

        else:
            print(
                f"""
            File '{self.file_path}' Validation: columns datatype check failed.
            
        """
            )
        return False

    def validate_column_value_set(self) -> bool:
        for column_value_set in self.validation_config.get("columns_value_set").items():
            self.df[column_value_set[0]] = self.df[column_value_set[0]].str.lower()
        ge_df = ge.from_pandas(self.df)
        flag = true
        for column_value_set in self.validation_config.get("columns_value_set").items():
            output = ge_df.expect_column_values_to_be_in_set(
                column_value_set[0], value_set=column_value_set[1]
            )
            if output.success is False:
                flag = false
                print(
                    f"""File column value in set validation failed: column name is {column_value_set[0]}"""
                )

        if flag == true:
            print(
                f"""
                File '{self.file_path}' Validation: columns value set check passed.
                """
            )
            return True

        else:
            print(
                f"""
            File '{self.file_path}' Validation: columns value set check failed.
            
        """
            )
        return False

    def validate_file(self):
        columns_count_flag = self.validate_columns_count()
        columns_datatype_flag = self.validate_column_data_type()
        columns_value_set_flag = self.validate_column_value_set()
        if (
            columns_count_flag is True
            and columns_datatype_flag is True
            and columns_value_set_flag is True
        ):
            print(f"{self.file_path}:All validations are passed")
            return True
        else:
            print(f"{self.file_path}:File validation failed!")
            raise ValueError()
            return False


test_file_validator = FileValidation("data/customer.csv", "csv", validation_config)

print(test_file_validator.validate_column_value_set())

