import s3fs
import os
import pandas as pd
import fsspec
from ydata_profiling import ProfileReport

# MinIO configuration
MINIO_ENDPOINT = "172.17.0.1:9000"
MINIO_ACCESS_KEY = "kStHEgiS0L8wSMHBoOq6"
MINIO_SECRET_KEY = "6uiWCp2tkHVA7dicuXawjI2fyhX5PtEKJwECSFaV"
MINIO_BUCKET = "mybucket"
PARQUET_FOLDER_PATH = "stock_data/19-11-2024"
REPORT_FILE_NAME = "pandas_profile_report.html"
REPORT_S3_PATH = f"{MINIO_BUCKET}/reports/{REPORT_FILE_NAME}" 
# Set up s3fs with MinIO credentials
fs = s3fs.S3FileSystem(
    key=MINIO_ACCESS_KEY,
    secret=MINIO_SECRET_KEY,
    client_kwargs={
        'endpoint_url': f'http://{MINIO_ENDPOINT}',  # Use http if no SSL
    }
)

# Construct the s3 folder path 
s3_folder_path = f'{MINIO_BUCKET}/{PARQUET_FOLDER_PATH}/'

# List all files in the directory
files = fs.glob(f'{s3_folder_path}*.parquet')

# Read all Parquet files from the folder into a single DataFrame
df = pd.concat([pd.read_parquet(f's3://{file}', filesystem=fs) for file in files])

# Show the DataFrame
print(df)


profile = ProfileReport(df,title=f"Stock Data Report")
local_report_path = "pandas_profile_report.html"
# Save the report as an HTML file
profile.to_file(local_report_path)


fs.put(local_report_path, REPORT_S3_PATH)  # Pass the file path directly

print(f"Report successfully uploaded to s3://{REPORT_S3_PATH}")