# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


import numpy as np
import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession
import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
from pyspark.sql.functions import col

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
import boto3
import pandas as pd


# Initialize an S3 client
s3 = boto3.client('s3', aws_access_key_id='YOUR_ACCESS_KEY', aws_secret_access_key='YOUR_SECRET_KEY')

# Specify S3 bucket information
bucket_name = 'YOUR_BUCKET_NAME'
file_key = 'path/to/your/file.csv'

# Specify a local file name to save the CSV file
local_file_name = 'local_file.csv'

try:
    # Attempt to download the file from S3
    s3.download_file(bucket_name, file_key, local_file_name)

except Exception as e:
    print(f"File download from S3 failed: {e}")

    # If the download fails, load the file path from the configuration file
    # Replace 'path_in_config_file' with the actual code to load the file path from your configuration file
    path_in_config_file = "path_to_config_file.txt"

    try:
        # Attempt to read the file path from the configuration file
        with open(path_in_config_file, 'r') as file:
            local_file_name = file.read().strip()

        print(f"Using file path from the configuration file: {local_file_name}")

    except Exception as e:
        print(f"Failed to read file path from the configuration file: {e}")

# Read the CSV file into a DataFrame
df = pd.read_csv(local_file_name)

import pytz
from datetime import datetimeimport pytz
from datetime import datetime

for column in df.columns:
    if pd.api.types.is_datetime64_any_dtype(df[column]):
        timestamp_column = column
        break

timestamp_column = 'timestamp_column'

# Specify the source time zone
source_timezone = pytz.timezone('US/Eastern')   # timezone to be specified

# Convert timestamps to UTC
df[timestamp_column] = df[timestamp_column].apply(lambda x: source_timezone.localize(x).astimezone(pytz.UTC))

df.set_index(timestamp_column, inplace=True)

df.to_csv('output_time_series_utc.csv', index=False)
































