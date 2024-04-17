import json
import pandas as pd
from DataManager._influxdatabase import APIHandler

token = "FN1XbuLPfmUJ5xFZpjdI9m0TRq1jeNEcmh305vVV9nexhHo7FTwPHNC9NWKUP4yxvu2qzGL8UaAjykZUiZkejA=="
org = "self"
url = "http://localhost:8086/"

api = APIHandler(url, token, org)

df = (api.fetch_time_series_data("2023-12-01", 
                                "2024-01-01", 
                                "1h", 
                                ["API_Gravity", "Catalyst_Activity_%", "ConversionEfficiency_%", "FlowRate_m3_hr"], 
                                ["CRUDE_AREA", "HYDRO_AREA"]))

for i in df.columns:
    df.rename(columns={i: f'{i[1]}_{i[0]}'}, inplace=True)

print(df)

df.index = pd.to_datetime(df.index)

json_data = df.to_json(orient="index")

parsed_json = json.loads(json_data)
formatted_json = json.dumps(parsed_json, indent=4)
print(formatted_json)

# api.fetch_time_series_data(startTime, stopTime, resamplingFreq, listofsensor_id, listofsensor_data)