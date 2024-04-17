# Industry-IQ

## Project Structure

<pre>
Industry-IQ
│
│   LICENSE
│
├───Alerts_Layer
│       app.py
│
├───Dashboard_Layer
│       app.py
│
├───DataBase_Layer
│       app.py
│
├───DataGenerator_Layer
│       app.py
│
├───InfluxDB
│   │   dbschema.txt
│   │   fetch_from_influxdb.py
│   │   influx_tag_manager.ipynb
│   │   push-to-influxdb-json.py
│   │   push_2_influxdb.py
│   │
│   └───DataManager
│       │   _influxdatabase.py
│       │   __init__.py
│       │
│       └───__pycache__
│               _influxdatabase.cpython-311.pyc
│               __init__.cpython-311.pyc
│
├───KML_Layer
│   │   app.py
│   │
│   └───src
│           data_ingestion.py
│           kpi_calculation.py
│
├───METADATA
│   ├───KML_layer
│   │       CRUDE_AREA.json
│   │       HYDRO_AREA.json
│   │       OVERALL_PLANT.json
│   │       UTILITIES_AREA.json
│   │
│   └───ML_Layer
│       └───Predection
│               CRUDE_AREA.json
│               HYDRO_AREA.json
│               OVERALL_PLANT.json
│               UTILITIES_AREA.json
│
├───MetaData_Layer
│       app.py
│
└───ML_Layer
    ├───AnomalyDetection
    │   │   app.py
    │   │
    │   └───Config
    │           config_example.json
    │
    ├───DataAnalysis
    │       app.py
    │
    ├───Forecasting
    │       app.py
    │
    ├───Modles
    │       CRUDE_AREA__Predection.pkl
    │
    └───Prediction
        │   app.py
        │
        └───src
                predection.py
</pre>
