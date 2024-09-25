from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO
import requests 
import json
import logging

def _get_stock_prices(url, symbol):
    
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers = api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])


def _store_prices(stock, symbol, symbol_name):
    logging.info(f"Received stock data: {stock}")
    if stock:
        # stock = json.loads(stock)
        # Proceed with storing the stock data
        minio = BaseHook.get_connection('minio')
        client = Minio(
            endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
            access_key=minio.login,
            secret_key=minio.password,
            secure=False
        )
        bucket_name = 'stock-market'
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
        # stock = json.loads(stock)
        # symbol = stock['meta']['symbol']
        data = json.dumps(stock, ensure_ascii=False).encode('utf8')
        objw = client.put_object(
            bucket_name = bucket_name,
            object_name=f'{symbol_name}/prices.json',
            data=BytesIO(data),
            length=len(data)
        )

        return f'{objw.bucket_name}/{symbol_name}'
    else:
        raise ValueError("No stock data received")


def write_metadata_to_minio(bucket_name, file_name, metadata):
    client = Minio(
        "host.docker.internal:9000",  # Replace with your MinIO server URL
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    
    # Convert metadata to a JSON string and then to a byte stream
    metadata_bytes = BytesIO(json.dumps(metadata).encode('utf-8'))
    
    # Upload the byte stream to Minio
    client.put_object(
        bucket_name, 
        file_name, 
        data=metadata_bytes, 
        length=len(json.dumps(metadata).encode('utf-8')), 
        content_type='application/json'
    )

def write_metadata():
    minio_client = Minio('host.docker.internal:9000', access_key='minio', secret_key='minio123', secure=False)
    metadata = {
        'buckets': {
            'stock_data': 'stock-market',  # Use your existing bucket
            'prediction_data': 'stock-market'  # Use the same bucket if needed
        },
        'locations': {
            'stocks': 'stock-market',
            'index': 'stock-market'
        }
    }
    write_metadata_to_minio('stock-market', 'metadata.json', metadata)


def read_metadata_from_minio(bucket_name, file_name):
    client = Minio('host.docker.internal:9000', access_key='minio', secret_key='minio123', secure=False)
    data = client.get_object(bucket_name, file_name).read().decode('utf-8')
    return json.loads(data)
