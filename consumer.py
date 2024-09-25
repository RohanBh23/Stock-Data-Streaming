from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime, timedelta

# List of topics (tickers)
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

# Kafka consumer configuration
consumer = KafkaConsumer(
    *tickers,  # Subscribe to all ticker topics
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Dictionary to store probabilities by time interval
probability_by_time = defaultdict(lambda: {'ticker': None, 'probability_of_profit': -float('inf')})

# Time interval (for example, every 5 minutes)
time_interval = timedelta(minutes=5)
last_check_time = datetime.now()

def process_message(message):
    data = message.value
    timestamp = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
    ticker = data['ticker']
    probability_of_profit = data['probability_of_profit']
    
    interval_start = timestamp - timedelta(minutes=(timestamp.minute % 5), seconds=timestamp.second, microseconds=timestamp.microsecond)
    
    if probability_of_profit > probability_by_time[interval_start]['probability_of_profit']:
        probability_by_time[interval_start] = {'ticker': ticker, 'probability_of_profit': probability_of_profit}

def print_highest_probability_ticker():
    global last_check_time
    now = datetime.now()
    if now - last_check_time >= time_interval:
        if probability_by_time:
            latest_interval = max(probability_by_time.keys())
            result = probability_by_time[latest_interval]
            print(f"Time Interval: {latest_interval.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Ticker with Highest Probability of Profit: {result['ticker']}")
            print(f"Probability of Profit: {result['probability_of_profit']}")
            print('---')
        last_check_time = now

if __name__ == "__main__":
    for message in consumer:
        process_message(message)
        print_highest_probability_ticker()
