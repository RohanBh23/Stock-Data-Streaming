import yfinance as yf
from kafka import KafkaProducer
import json
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

# Define tickers for the top 100 NASDAQ stocks
tickers = [
    'AAPL', 'MSFT', 'AMZN', 'NVDA', 'GOOGL', 'GOOG', 'META', 'TSLA', 'AVGO', 'PEP',
    'COST', 'CSCO', 'ADBE', 'TXN', 'NFLX', 'QCOM', 'TMUS', 'AMD', 'INTC', 'INTU',
    'AMGN', 'SBUX', 'ISRG', 'HON', 'MRNA', 'AMAT', 'BKNG', 'ADP', 'ADI', 'MDLZ',
    'PYPL', 'GILD', 'LRCX', 'REGN', 'FISV', 'VRTX', 'MU', 'CSX', 'KLAC', 'PANW',
    'SNPS', 'ADSK', 'ORLY', 'MNST', 'ASML', 'CTAS', 'MELI', 'NXPI', 'ABNB', 'IDXX',
    'FTNT', 'WBA', 'CDNS', 'LULU', 'BIIB', 'AEP', 'KDP', 'XEL', 'TEAM', 'ANSS',
    'ROST', 'VRSK', 'MAR', 'ODFL', 'PAYX', 'PCAR', 'MCHP', 'KHC', 'SIRI', 'DLTR',
    'EXC', 'ALGN', 'EA', 'CTSH', 'VRSN', 'SGEN', 'BIDU', 'PDD', 'MTCH', 'AZN',
    'ILMN', 'ZS', 'CRWD', 'SPLK', 'DOCU', 'OKTA', 'DDOG', 'ZM', 'MDB', 'CRWD',
    'SPLK', 'SNOW', 'ROKU', 'CHTR', 'DOCU', 'OKTA', 'TTD', 'NET', 'DDOG', 'BILL'
]

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    compression_type='gzip',
    batch_size=16384,
    linger_ms=5
)

# Function to fetch real-time data
def fetch_stock_data(tickers):
    data = {}
    for ticker in tickers:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d", interval="1m")
        data[ticker] = hist
    return data

# Function to process stock data
def process_stock_data(ticker, df):
    window_size = 5  # 5 minutes
    df['Percentage Change'] = df['Close'].pct_change().dropna()
    
    results = []
    for i in range(window_size, len(df)):
        window_df = df.iloc[i-window_size:i]
        percentage_changes = window_df['Percentage Change'].dropna()
        current_price = df['Close'].iloc[i]
        
        if not percentage_changes.empty:
            split_index = (np.abs(percentage_changes - current_price)).idxmin()
            probability_of_profit = split_index / len(percentage_changes)
        else:
            probability_of_profit = None
        
        result = {
            'timestamp': df.index[i].strftime('%Y-%m-%d %H:%M:%S'),
            'ticker': ticker,
            'current_price': current_price,
            'probability_of_profit': probability_of_profit
        }
        results.append(result)
    return results

# Function to send processed data to Kafka
def send_to_kafka(ticker, data):
    for record in data:
        producer.send(ticker, record)
    producer.flush()

if __name__ == "__main__":
    while True:
        stock_data = fetch_stock_data(tickers)
        for ticker, df in stock_data.items():
            # try :
                
            processed_data = process_stock_data(ticker, df)
            send_to_kafka(ticker, processed_data)
            # except:
            #     print("Error with topic : ", ticker)
        time.sleep(60)
