from file_validator import FileValidation

from os import listdir
from os.path import isfile, join
import pandas as pd

consumption_data_files_validation_config = {
    "total_columns_count": 3,
    "columns_datatype": {
        "AESTTime": "object",
        # set this column datatype to be object instead of datetime as read_csv can only contain strings, integers and floats.
        "Quantity": "float64",
        "Unit": "object",
    },
    "columns_value_set": {"Unit": ["kwh", "wh", "mwh"]},
    # always set values in lower case
}

consumption_data_path = "data/consumption_data"
consumption_data_file_format = "csv"

nmi_info_data_path = "data/nmi_info.csv"
nmi_info_df = pd.read_csv(nmi_info_data_path)

consumption_data_files = [
    f for f in listdir(consumption_data_path) if isfile(join(consumption_data_path, f))
]
df_list = []

for file in consumption_data_files:
    file_validator = FileValidation(
        file_path=f"{consumption_data_path}/{file}",
        file_format=consumption_data_file_format,
        validation_config=consumption_data_files_validation_config,
    )
    # file_validator.validate_file()
    file_df = pd.read_csv(f"{consumption_data_path}/{file}")
    file_df["Nmi"] = file.replace(".csv", "")
    df_list.append(file_df)
    # break


print(len(df_list))
consumption_data_df_unioned = pd.concat(df_list, ignore_index=True)
print(consumption_data_df_unioned.info())
print(consumption_data_df_unioned.head(10))

print(nmi_info_df.info())

joined_df = consumption_data_df_unioned.merge(nmi_info_df, on="Nmi", how="inner")
print(joined_df.info())
