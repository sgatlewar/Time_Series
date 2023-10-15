import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
sns.set_theme(style="ticks", color_codes=True)
import mpl_toolkits
from sklearn.cluster import DBSCAN
import boto3


# Set the S3 file URL
s3_file_url = "D:\outofblue\forecast_execution_files\01_timeseries_total_orders.csv"

# Read the CSV file from S3 into a pandas DataFrame
df = pd.read_csv(s3_file_url)






import boto3
# AWS credentials
aws_access_key_id = "AKIAZBLB2O5YHNQQOJ2A"
aws_secret_access_key = "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"
bucket_name = 'capstone-nikhilesh-mahesh'
s3_path = ''  # Optional: specify a path within the bucket

# Create a new S3 session
s3 = boto3.resource('s3',
                    aws_access_key_id,
                    aws_secret_access_key)

# Upload the CSV file to S3
s3.Bucket(bucket_name).upload_file('xyz.csv', '' + 'xyz.csv')


