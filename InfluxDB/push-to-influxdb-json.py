import numpy as np
import pandas as pd
import os, time
import configparser
import influxdb_client
import json
from decimal import Decimal
from datetime import datetime
from itertools import product
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client import BucketsApi, Bucket, BucketRetentionRules
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

from DataManager._influxdatabase import APIHandler

token = "FN1XbuLPfmUJ5xFZpjdI9m0TRq1jeNEcmh305vVV9nexhHo7FTwPHNC9NWKUP4yxvu2qzGL8UaAjykZUiZkejA=="
org = "self"
url = "http://localhost:8086/"

api = APIHandler(url, token, org)

###======================================================================================================###

## Provide the folder location here where the data is stored

folder_path = r"C:\Users\hp\Desktop\CodeSpace\Projects\Industry-IQ\Data\data"

c = 0 
files = os.listdir(folder_path)
files.sort()

hh = []

for i in files:
    i = i.replace('.json', '')
    hh.append(i)

print(hh)
for filename in files[:]:
# for filename in os.listdir(folder_path):
    if filename.endswith(".json"):

        c += 1
        json_filepath = os.path.join(folder_path, filename)

        try:
            with open(json_filepath, 'r') as file:
                data = json.load(file)

            if 'Table1' in data:
                df = pd.DataFrame(data['Table1'])
            else:
                df = pd.DataFrame(data)

            tag_name = df['TagName'].iloc[0]

            df = df[['TimeStamp', 'Value']]
            df = df.rename(columns={'Value': tag_name})
            df = df.rename(columns={'TimeStamp': 'Timestamp'})

            df['Timestamp'] = pd.to_datetime(df['Timestamp'], format="%d-%m-%Y %I:%M:%S %p")
            df['Timestamp'] = df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df = df.rename(columns={f'{tag_name}': '_value'})
            df['sensor_id'] = f"{tag_name.replace('GWA.', '')}"
            df['sensor_data'] = 'raw_sensor_data'
            df['_value'] = pd.to_numeric(df['_value'], errors='coerce')
            df['_value'] = df['_value'].astype(float) 
            print(f"{c} \t {datetime.now()} \t {df.iloc[0,2]} \t {(df.shape[0])}", end="\n")
            api.write_time_series_data(df, "iocl_paradip_dev", "tag_data", [df.columns[3], df.columns[2]])
            # api.write_time_series_data(df, "iocl_guwahati", "tag_data", [df.columns[3], df.columns[2]])
            print(df)

        except json.JSONDecodeError as jde:
            print(f"JSON decoding error at line {jde.lineno}, column {jde.colno}: {jde.msg}")
        except FileNotFoundError:
            print(f"File {json_filepath} not found.")
        except KeyError as e:
            print(f"Key {e} not found in the JSON or DataFrame.")
        except Exception as e:
            print(f"An error occurred: {e}")