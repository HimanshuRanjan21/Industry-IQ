from flask import Flask, request, render_template
import json
from ML_Layer.Prediction.src.predection import PredictionModel

app = Flask(__name__)

# Route for home page
@app.route('/')
def home():
    return 'None'

# Route for predicting data
@app.route('/predictdata/<start_time>-<end_time>-<Area_id>', methods=['GET', 'POST'])
def predict_data(start_time, end_time, Area_id):
    try:
        file_path = f'METADATA/ML_Layer/{Area_id}.json'
        with open(file_path, 'r') as file:
            config = json.load(file)
        pred = PredictionModel(config)
        pred.fit(start_time=start_time, end_time=end_time)

        return 'Prediction Done'
    except:
        return "Failed"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
