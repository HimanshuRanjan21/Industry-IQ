import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error
import pickle
import os
import json

import sys
sys.path.append('c:\\Users\\hp\\Desktop\\CodeSpace\\Projects\\Industry-IQ\\Industry-IQ')
from InfluxDB.DataManager._influxdatabase import APIHandler

token = "FN1XbuLPfmUJ5xFZpjdI9m0TRq1jeNEcmh305vVV9nexhHo7FTwPHNC9NWKUP4yxvu2qzGL8UaAjykZUiZkejA=="
org = "self"
url = "http://localhost:8086/"

api = APIHandler(url, token, org)

Models_path = "ML_Layer\\Modles"

class ForecastModel:
    def __init__(self, model_config):
        self.model_config = model_config
        self.model = None
        self.model_name = self.model_config["AREA_id"] + '__Forecast.pkl'
        self.model_file = os.path.join(Models_path, self.model_name)

    def injest_data(self,start_time,end_time):
        print("📖 Read Data")
        print(self.model_config["data_to_fetch"]["credentials"]["tag_name"])
        print([f'{(self.model_config["AREA_id"])}'])
        df = api.fetch_time_series_data(start_time, 
                                end_time, 
                                "1h", 
                                self.model_config["data_to_fetch"]["credentials"]["tag_name"], 
                                [f'{(self.model_config["AREA_id"])}'])
        for i in df.columns:
            df.rename(columns={i: f'{i[0]}'}, inplace=True)
        print(df)
        return df

    def write_data(self, final_df):
        print("✍🏻 Write Data")
        print(f"{datetime.now()} \t {final_df.iloc[0,2]} \t {(final_df.shape[0])}", end="\n")
        final_df = final_df.reset_index()
        print(final_df)
        api.write_time_series_data(final_df, 
                                   self.model_config["output_storage"]["credentials"]["bucket"], 
                                   self.model_config["output_storage"]["credentials"]["measurement"], 
                                   [final_df.columns[3], final_df.columns[2]])
        return None

    def fit(self,start_time,end_time):

        df=self.injest_data(start_time,end_time)

        
        # col = self.model_config["ML_implementation"]["Independent_variable"]
        # df = df[col]

        with open(self.model_file, 'rb') as f:
            model = pickle.load(f)

        Forecast = model.predict(df)

        final_df=pd.DataFrame()
        final_df.index=df.index
        final_df['_value']=Forecast
        final_df['sensor_id']='Forecasted_Level'
        final_df['sensor_data']='CRUDE_AREA'

        self.write_data(final_df)

        print(Forecast)



if __name__=='__main__':

    file_path = 'METADATA/ML_Layer/Forecast/CRUDE_AREA.json'


    with open(file_path, 'r') as file:
        config = json.load(file)

    pred=ForecastModel(config)

    pred.fit(start_time="2023-12-01", end_time="2024-01-01")


