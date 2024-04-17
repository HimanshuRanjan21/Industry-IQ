# Importing Necessary Libraries
import numpy as np
import pandas as pd
import os, time
import configparser
import influxdb_client

from datetime import datetime
from itertools import product
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client import BucketsApi, Bucket, BucketRetentionRules
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

allowed_influxDB_pairs = {
    'measurement': ['tag_data', 'KPIs', 'lab_data'],
    'field': ['raw_value', 'cleaned_value', 'value', 'KPI_value'],
    'tag': ['sensor_id', 'KPI_name', 'type_of_property']
}

class APIHandler:
    """
    APIHandler class to interact with InfluxDB.

    This class provides methods for the following operations:
    - Write time series data into a specific InfluxDB bucket

    Attributes:
        url (str): The URL of the InfluxDB server.
        token (str): The authentication token for accessing InfluxDB.
        org (str): The organization associated with InfluxDB data.
        client (InfluxDBClient): An InfluxDB client.
        buckets_api (BucketsApi): An InfluxDB BucketsApi object.
    """

    def __init__(self, url: str, token: str, org: str):
        """
        Initialize the APIHandler instance.

        Args:
            url (str): The URL of the InfluxDB server.
            token (str): The authentication token for accessing InfluxDB.
            org (str): The organization associated with InfluxDB data.

        Returns:
            None
        """
        
        # Assign arguments to instance variables
        self.url = url
        self.token = token
        self.org = org

        # Create an InfluxDB client and bucket API
        self.client = influxdb_client.InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self.buckets_api = self.client.buckets_api()

    def write_time_series_data(self, 
                            df: pd.DataFrame, 
                            bucket: str, 
                            measurement: str, 
                            tag_name: list
                            ) -> None:
        """
        Writes time series data to an InfluxDB bucket.

        Args:
            df (pd.DataFrame): The data to be written to InfluxDB. It should have a 'Timestamp' column in 
                            '%Y-%m-%d %H:%M:%S' format.
            bucket (str): The name of the bucket in InfluxDB to which data will be written.
            measurement (str): The name of the measurement or table within the bucket.
            tag_name (list): A list of tags to be assigned to the data.

        Returns:
            None
        """

        if 'Timestamp' not in df.columns:
            df.rename(columns={df.columns[0]: 'Timestamp'}, inplace=True)

        df['Timestamp'] = pd.to_datetime(df['Timestamp'], format="%Y-%m-%d %H:%M:%S")

        df_grouped = df.groupby(df['Timestamp']).cumcount()
        df['Timestamp'] += pd.to_timedelta(df_grouped, unit='s')
        start = time.time()
        with InfluxDBClient(url=self.url, token=self.token, org=self.org) as client:
            with client.write_api(write_options=WriteOptions(batch_size=400_000,
                                                            flush_interval=1_000,
                                                            jitter_interval=500,
                                                            retry_interval=2_000,
                                                            max_retries=5,
                                                            max_retry_delay=30_000,
                                                            max_close_wait=300_000,
                                                            exponential_base=2)) as write_api:
                try:
                    write_api.write(
                        bucket=bucket,
                        record=df,
                        data_frame_measurement_name=measurement,
                        data_frame_tag_columns=tag_name,
                        data_frame_field_columns=["_value"],
                        data_frame_timestamp_column="Timestamp",
                    )
                    print("✅", end="\t")  # Success indicator
                except Exception as e:
                    # Here, you can log the exception details for debugging purposes
                    # For example, you could write the errors to a log file
                    with open("error_log.txt", "a") as file:
                        file.write(f"Exception occurred: {e}\n")
                    # If you still want to see the error message in the console, uncomment the next line
                    # print(f"Error occurred: {e}")

        end = time.time()
        print(f"{end - start}")

    def fetch_time_series_data(self, 
                        timeRangeStart: str,
                        timeRangeStop: str,
                        resampling_freq: str,
                        sensor_id: list,
                        sensor_data: list
                        ) -> pd.DataFrame:
        """
        Fetches time series data from InfluxDB for a given time range, resampling frequency,
        bucket, measurement, and tag.

        Args:
            timeRangeStart (str): Start time for the data in the format 'YYYY-MM-DDTHH:MM:SSZ'.
            timeRangeStop (str): End time for the data in the same format.
            resampling_freq (str): The frequency at which the data needs to be resampled.
                                  This should be a string such as '1m' for one minute.
            bucket (str): The name of the bucket in InfluxDB from which the data is to be fetched.
            measurement (str): The measurement within the bucket from which data is to be fetched.
            tag (dict): A dictionary containing tags for fetching data. The keys are the tag names
                        and the values are lists of elements with the tag name.
            offset (str): Time offset to be added to the index.

        Returns:
            pd.DataFrame: A DataFrame containing the fetched time series data.
        """

        bucket = "plant"
        measurement = "tag_data"
        offset = "0m"
        
        # create an instance of the InfluxDBClient
        client = InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=100000)
        
        # initialize a new dataframe to store the results
        df_all = pd.DataFrame()

        tag = {
            "sensor_id" : sensor_id,
            "sensor_data" : sensor_data
        }

        # split the tag dictionary into separate lists of keys and values
        keys = list(tag.keys())
        values = list(tag.values())
        ii = 0
        for combination in product(*values):
            output1 = []

            # build the flux query filter for each combination of tags
            for i in range(len(keys)):
                output = ''.join(f'r["{keys[i]}"] == "{combination[i]}"')
                tag_id = combination
                output1.append(output)
            
            filter_condition = ' and '.join(output1)
            print(f'\n {ii} {datetime.now()} Filter Condition: {filter_condition}\n', end=" ")
            ii+=1
            # formulate the flux query
            if resampling_freq=="0m":
                flux_query = f'''
                    from(bucket: "{bucket}")
                    |> range(start: time(v: "{timeRangeStart}"), stop: time(v: "{timeRangeStop}"))
                    |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                    |> filter(fn: (r) => {filter_condition})
                    |> filter(fn: (r) => r["_field"] == "_value")
                '''
            else:
                flux_query = f'''
                    from(bucket: "{bucket}")
                    |> range(start: time(v: "{timeRangeStart}"), stop: time(v: "{timeRangeStop}"))
                    |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                    |> filter(fn: (r) => {filter_condition})
                    |> filter(fn: (r) => r["_field"] == "_value")
                    |> aggregateWindow(every: {resampling_freq}, fn: mean, createEmpty: false)
                    |> yield(name: "mean")
                '''
            try:
                print(flux_query)
                # execute the flux query
                result = client.query_api().query(org=self.org, query=flux_query)
                print(result)
                print(type(result))
                # convert the result into a dataframe
                data = [record.values for table in result for record in table.records]
                df = pd.DataFrame(data)

                # if dataframe is not empty, append it to the master dataframe
                if not df.empty:
                    series = pd.Series(data=df['_value'].values, 
                                    index=pd.to_datetime(df['_time']), 
                                    name="tag_name[0]")

                    df_all = pd.concat([df_all, series], axis=1)
                    last_column_name = df_all.columns[-1]
                    df_all.rename(columns={last_column_name: tag_id}, inplace=True)
                    print("✅")
                else:
                    print(f"❌")

            except ApiException as e:
                print(f"Exception when calling QueryApi->query: {str(e)}")

        df_all.rename_axis('Timestamp', inplace=True)
        
        print(f"Bucket: {bucket} >> Measurement: {measurement}")

        df_all.index = pd.to_datetime(df_all.index).strftime('%Y-%m-%d %H:%M')
        df_all.index = pd.to_datetime(df_all.index)
        
        df_all.index = pd.to_datetime(df_all.index) + pd.Timedelta(offset)
        df_all = df_all.sort_values(by="Timestamp")

        return df_all
    
# Create a ConfigParser instance
config = configparser.ConfigParser()

# Read the config file
current_file_path = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file_path)
config.read(os.path.join(current_directory, 'config.ini'))

# Get the username and password from the 'database' section

token = "FN1XbuLPfmUJ5xFZpjdI9m0TRq1jeNEcmh305vVV9nexhHo7FTwPHNC9NWKUP4yxvu2qzGL8UaAjykZUiZkejA=="
org = "self"
url = "http://localhost:8086/"

api = APIHandler(url, token, org)

##--------------------------------------------------------------------------------------------------------------------------------------##

if __name__ == "__main__":
 
    print("Algo8")
