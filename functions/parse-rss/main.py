import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
import pandas as pd
import requests
import json
import os
from io import BytesIO
import datetime
import uuid

# Settings
project_id = 'financial-pipeline-group-6'
bucket_name = 'financial-pipeline-group-6-bucket'
finnhub_api_key = os.getenv('FINNHUB_API_KEY')
ingest_timestamp = pd.Timestamp.now()

############################################################### helpers

def fetch_finnhub_data(symbol, from_timestamp, to_timestamp):
    """Fetch data from the FinnHub API for a given symbol."""
    api_url = f"https://finnhub.io/api/v1/stock/candle?symbol={symbol}&resolution=D&from={from_timestamp}&to={to_timestamp}&token={finnhub_api_key}"
    
    try:
        response = requests.get(api_url)
        data = response.json()
        if data['s'] != 'ok':
            raise ValueError(f"API returned error for symbol {symbol}: {data}")
        return data
    except Exception as e:
        print(f"Error fetching data from FinnHub: {e}")
        raise

############################################################### main task

@functions_framework.http
def task(request):
    try:
        # Parse the request data
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"error": "Invalid JSON payload"}, 400

        # Get the job_id and GCS base
        job_id = request_json.get('job_id', str(uuid.uuid4()))  # Default to a new UUID if job_id not provided
        bucket_name = request_json.get('bucket_name', 'financial-pipeline-group-6-bucket')  # Default bucket name
        gcs_base = f'gs://{bucket_name}/jobs/{job_id}/'

        # FinnHub Data Extraction Parameters
        symbol = request_json.get('symbol', 'AAPL')
        from_timestamp = request_json.get('from_timestamp', 1672531200)  # January 1, 2023
        to_timestamp = request_json.get('to_timestamp', 1704067200)  # January 1, 2024

        # Fetch data from FinnHub API
        finnhub_data = fetch_finnhub_data(symbol, from_timestamp, to_timestamp)

        # Convert the fetched data into a DataFrame
        finnhub_df = pd.DataFrame({
            'date': pd.to_datetime(finnhub_data['t'], unit='s'),
            'open': finnhub_data['o'],
            'high': finnhub_data['h'],
            'low': finnhub_data['l'],
            'close': finnhub_data['c'],
            'volume': finnhub_data['v'],
            'job_id': job_id
        })

        finnhub_df['ingest_timestamp'] = ingest_timestamp

        # Convert DataFrame to Parquet in memory using BytesIO
        output_stream = BytesIO()
        finnhub_df.to_parquet(output_stream, engine='pyarrow')
        output_stream.seek(0)

        # Write the DataFrame to GCS in Parquet format
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob_name = f"jobs/{job_id}/{symbol}_finnhub_data.parquet"
        blob = bucket.blob(blob_name)

        # Upload the file from memory
        blob.upload_from_string(output_stream.getvalue(), content_type='application/octet-stream')

        ########################### return
        return {
            "symbol": symbol,
            "finnhub_data_path": blob_name,
            "num_entries": len(finnhub_df),
            "job_id": job_id
        }, 200

    except Exception as e:
        print(f"Error: {e}")
        return {"error": str(e)}, 500
