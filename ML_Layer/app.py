from flask import Flask, request, render_template
import json

import sys
sys.path.append('c:\\Users\\hp\\Desktop\\CodeSpace\\Projects\\Industry-IQ\\Industry-IQ')
from ML_Layer.Prediction.src.predection import PredictionModel
from ML_Layer.Forecasting.src.forecast import ForecastModel

app = Flask(__name__)

# Route for home page
@app.route('/')
def home():
    return 'None'

# Route for predicting data
@app.route('/predictdata/<start_time>/<end_time>/<Area_id>', methods=['GET', 'POST'])
def predict_data(start_time, end_time, Area_id):
    start_time=str(start_time)
    end_time=str(end_time)
    Area_id=str(Area_id)

    print("⏳ PREDICT ",start_time,end_time,Area_id)
    try:
        file_path = f'METADATA/ML_Layer/Predection/{Area_id}.json'
        with open(file_path, 'r') as file:
            config = json.load(file)
        pred = PredictionModel(config)
        pred.fit(start_time=start_time, end_time=end_time)

        return 'Prediction Done 📊'
    except:
        return "Failed"
    
@app.route('/forecastdata/<start_time>/<end_time>/<Area_id>', methods=['GET', 'POST'])
def forecast_data(start_time, end_time, Area_id):
    start_time=str(start_time)
    end_time=str(end_time)
    Area_id=str(Area_id)

    print("⏳ FORECAST ",start_time,end_time,Area_id)
    # try:
    file_path = f'METADATA/ML_Layer/Forecast/{Area_id}.json'
    with open(file_path, 'r') as file:
        config = json.load(file)
    pred = ForecastModel(config)
    pred.fit(start_time=start_time, end_time=end_time)

    return 'Forecast Done 📈'
    # except:
    #     return "Failed"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)


# /predictdata/2023-12-01-2024-01-01-CRUDE_AREA