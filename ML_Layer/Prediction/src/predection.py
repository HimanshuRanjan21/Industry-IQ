import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error
import pickle
import os
import json

Models_path = "ML_Layer/Models"

class PredictionModel:
    def __init__(self, model_config):
        self.model_config = model_config
        self.model = None
        self.model_name = self.model_config["AREA_id"] + '__Predection.pkl'
        self.model_file = os.path.join(Models_path, self.model_name)

    def injest_data(self,start_time,end_time):
        df = None
        return df

    def write_data(self, prediction):
        return None

    def fit(self,start_time,end_time):

        df=self.injest_data(start_time,end_time)

        
        col = self.model_config["ML_implementation"]["Independent_variable"]
        df = df[col]

        with open(self.model_file, 'rb') as f:
            model = pickle.load(f)

        prediction = model.predict(df)

        self.write_data(prediction)

        return prediction



if __name__=='__main__':

    file_path = 'METADATA/ML_Layer/CRUDE_AREA.json'


    with open(file_path, 'r') as file:
        config = json.load(file)

    pred=PredictionModel(config)

    pred.fit(start_time='', end_time='')


