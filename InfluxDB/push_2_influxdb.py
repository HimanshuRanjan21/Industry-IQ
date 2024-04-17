# Importing Necessary Libraries
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

def push_data_to_influxdb(folder_path):
    
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
        if filename.endswith(".csv"):

            c += 1
            json_filepath = os.path.join(folder_path, filename)

            try:
                with open(json_filepath, 'r') as file:
                    data = pd.read_csv(file, index_col="Unnamed: 0")

                if 'Table1' in data:
                    df = pd.DataFrame(data['Table1'])
                else:
                    df = pd.DataFrame(data)
                
                print("\n\n\n\n", file)

                for vn in df.columns:
                    gf = pd.DataFrame(df[vn])
                    gf.index = pd.to_datetime(gf.index)
                    gf['sensor_id'] = f"{vn}"
                    gf = gf.rename(columns={f'{vn}': '_value'})
                    gf['sensor_data'] = f'{filename[:-4].upper()}'
                    gf = gf.reset_index()
                    gf = gf.rename(columns={'index': 'Timestamp'})
                    gf['_value'] = pd.to_numeric(gf['_value'], errors='coerce')
                    gf['_value'] = gf['_value'].astype(float) 
                    print(f"{filename[:-4]} \t {datetime.now()} \t {gf.iloc[0,2]} \t {(gf.shape[0])}", end="\n")
                    api.write_time_series_data(gf, "plant", "tag_data", [gf.columns[3], gf.columns[2]])
                    print(gf)

            except Exception as e:
                print(f"An error occurred: {e}")