import pandas as pd
import glob
import datetime
import sys
import os
from pprint import pprint

HEADERS = ["email", "create_count", "read_count", "update_count", "delete_count"]
OPERATIONS = ["CREATE", "READ", "UPDATE", "DELETE"]
result_dict = {}

input_date = sys.argv[1]
start_date = datetime.datetime.strptime(input_date, "%Y-%m-%d")

output_file = f".\\output\\{input_date}.csv"
if not os.path.exists(os.path.dirname(output_file)):
    os.makedirs(os.path.dirname(output_file))

input_files = list(filter(lambda item: datetime.datetime.strptime(item.split("\\")[-1].split(".")[0], "%Y-%m-%d") > start_date,
                          glob.glob(".\\input\\*.csv")))[:7]

for file_path in input_files:
    data = pd.read_csv(file_path, index_col=False, header=None)
    data.rename(columns={0: "email", 1: "operation", 2: "date_time"}, inplace=True)
    operation_counts = data.groupby(["email", "operation"]).count().reset_index().pivot_table(values="date_time", index="email", columns="operation")
    for email in data["email"].unique():
        if email not in result_dict.keys():
            result_dict[email] = {}
        for operation in OPERATIONS:
            if operation not in result_dict[email].keys():
                result_dict[email][operation] = 0
            result_dict[email][operation] += operation_counts.loc[email][operation].astype(int)

template = dict().fromkeys(HEADERS, "")
to_csv_lst = [[key, *list(result_dict[key].values())] for key in result_dict.keys()]
final_frame = pd.DataFrame(to_csv_lst)
final_frame.to_csv(output_file, index=False, header=False)
