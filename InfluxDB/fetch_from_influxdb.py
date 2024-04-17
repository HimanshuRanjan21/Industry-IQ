from DataManager._influxdatabase import APIHandler

token = "FN1XbuLPfmUJ5xFZpjdI9m0TRq1jeNEcmh305vVV9nexhHo7FTwPHNC9NWKUP4yxvu2qzGL8UaAjykZUiZkejA=="
org = "self"
url = "http://localhost:8086/"

api = APIHandler(url, token, org)

print(api.fetch_time_series_data("01-12-2023", 
                                "01-01-2024", 
                                "1h", 
                                ["API_Gravity", "Catalyst_Activity_%", "ConversionEfficiency_%", "FlowRate_m3_hr"], 
                                ["CRUDE_AREA", "HYDRO_AREA"]))

# api.fetch_time_series_data(startTime, stopTime, resamplingFreq, listofsensor_id, listofsensor_data)