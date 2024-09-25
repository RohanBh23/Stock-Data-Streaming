from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from minio import Minio
import requests

from airflow.models.xcom_arg import XComArg

from include.stock_market.tasks import _get_stock_prices
from include.stock_market.tasks import _store_prices, read_metadata_from_minio
from include.stock_market.tasks import write_metadata
from include.stock_market.preprocessing import preprocess_stock_data
# from include.stock_market.preprocessing import preprocess_stock_data
# from spark.stock_transform import 

SYMBOLS = ['AAPL', 'GOOG', 'MSFT', 'TSLA', 'AMZN', '^GSPC', '^NDX']

@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=['stock_market']
)
def stock_market():
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    with TaskGroup("fetch_and_store_stock_data", tooltip="Fetch and store stock prices for multiple symbols") as stock_data_group:
        for symbol in SYMBOLS:
            if symbol.startswith('^'):
                symbol_name = symbol[1:]
            else:
                symbol_name = symbol

            get_stock_prices = PythonOperator(
                task_id=f'get_stock_prices_{symbol_name}',
                python_callable=_get_stock_prices,
                op_kwargs={'url': '{{ task_instance.xcom_pull(task_ids="is_api_available") }}', 'symbol': symbol}
            )
            
            # Sol 1 --> Did not work
            # store_prices = PythonOperator(
            #     task_id=f'store_prices_{symbol_name}',
            #     python_callable=_store_prices,
            #     op_kwargs={'stock': '{{ task_instance.xcom_pull(task_ids="get_stock_prices_' + symbol_name + '")}}', 'symbol': symbol, 'symbol_name': symbol_name}
            # )

            # # Set the task dependencies within the TaskGroup
            # get_stock_prices >> store_prices


            # Sol 2 --> Worked
            # Use XComArg to fetch the stock prices output for the store_prices task
            stock_data = XComArg(get_stock_prices)

            # Store the prices
            store_prices = PythonOperator(
                task_id=f'store_prices_{symbol_name}',
                python_callable=_store_prices,
                op_kwargs={'stock': stock_data, 'symbol': symbol, 'symbol_name': symbol_name}
            )

            # Set task dependencies within the TaskGroup
            get_stock_prices >> store_prices            

    # Define the write_metadata task after the TaskGroup finishes
    write_metadata_task = PythonOperator(
        task_id="write_metadata",
        python_callable=write_metadata
    )

    # Set task dependencies outside the TaskGroup
    is_api_available() >> stock_data_group >> write_metadata_task

stock_market()


@dag(
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=['stock_prediction']
)
def stock_prediction_dag():
    
    @task
    def fetch_metadata():
        # This task will read metadata from MinIO bucket
        metadata = read_metadata_from_minio('stock-market', 'metadata.json')
        return metadata

    with TaskGroup("preprocess_stock_data", tooltip="Preprocess stock prices for multiple symbols") as preprocess_group:
        minio_client = Minio('host.docker.internal:9000', access_key='minio', secret_key='minio123', secure=False)

        for symbol in SYMBOLS:
            symbol_name = symbol[1:] if symbol.startswith('^') else symbol

            # Create individual tasks for each symbol's preprocessing step
            preprocess_task = PythonOperator(
                task_id=f"preprocess_data_{symbol_name}",
                python_callable=preprocess_stock_data,
                op_kwargs={
                    "symbol": symbol,
                    "minio_bucket": "stock-market",
                    "minio_client": minio_client
                }
            )
    
    # Set task dependencies outside of TaskGroup
    metadata = fetch_metadata()
    metadata >> preprocess_group

stock_prediction_dag()


# def stock_prediction_dag():
    
#     @task
#     def fetch_metadata():
#         metadata = read_metadata_from_minio('stock-market', 'metadata.json')
#         return metadata
    
#     # @task
#     # def fetch_and_preprocess_data(metadata):
        
        
#     #     # Using TaskGroup inside the DAG
#     with TaskGroup("preprocess_stock_data", tooltip="Preprocess stock prices for multiple symbols") as preprocess_group:
#         minio_client = Minio('host.docker.internal:9000', access_key='minio', secret_key='minio123', secure=False)
#         for symbol in SYMBOLS:
#             symbol_name = symbol[1:] if symbol.startswith('^') else symbol

#             # Create individual tasks for each symbol
#             preprocess_task = PythonOperator(
#                 task_id=f"preprocess_data_{symbol_name}",
#                 python_callable=preprocess_stock_data,
#                 op_kwargs={
#                     "symbol": symbol,
#                     "minio_bucket": "stock-market",
#                     "minio_client": minio_client
#                 }
#             )
#         # return "Preprocessing complete"

#             fetch_metadata >> fetch_and_preprocess_data

# stock_prediction_dag()

