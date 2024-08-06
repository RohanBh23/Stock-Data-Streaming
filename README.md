# Stock-Data-Streaming

### Real-Time Stock Data Stream Processing leveraginh Kafka

**Overview**

This project involves building a real-time stock data processing system that integrates with Apache Kafka. The system fetches live stock price data for the top 100 NASDAQ stocks from Yahoo Finance, processes this data to compute percentage changes and a probability of profit metric, and publishes the results to Kafka topics. Each stock is represented by its own Kafka topic, and the system ensures robust handling of missing or erroneous data.

**Components**

1. **Data Fetching**:
   - **Source**: Yahoo Finance
   - **Interval**: 1-minute historical data
   - **Data**: Closing prices for the top 100 NASDAQ stocks.

2. **Data Processing**:
   - **Metrics Computed**:
     - **Percentage Change**: Daily percentage change in closing prices.
     - **Probability of Profit**: Calculated as the index of the closest percentage change to the current price divided by the total number of records in a 5-minute window.
   - **Window Size**: 5 minutes

3. **Kafka Integration**:
   - **Producer**: Publishes processed stock data to Kafka topics, each representing a different stock ticker.
   - **Consumer**: (Future implementation) Subscribes to all topics to identify and print the stock with the highest probability of profit for each time interval.

4. **Error Handling**:
   - Handles cases where stock data is missing or tickers are delisted.
   - Logs errors related to data fetching and processing.

**Features**

- **Real-Time Data Processing**: Fetches and processes stock data every minute.
- **Dynamic Topic Handling**: Creates and manages Kafka topics for each stock ticker dynamically.
- **Efficient Data Handling**: Uses efficient data structures and algorithms to process and publish stock data.
- **Robust Error Handling**: Includes mechanisms to handle missing data and errors gracefully.

**Technology Stack**

- **Python**: For data processing, integration with Yahoo Finance, and Kafka.
- **yfinance**: To fetch real-time stock price data from Yahoo Finance.
- **pandas & numpy**: For data manipulation and calculation.
- **Kafka**: For real-time data streaming and topic management.

**Usage**

1. **Run the Producer**: Execute the script to start fetching, processing, and publishing stock data to Kafka topics.
2. **Future Enhancements**: Implement a Kafka consumer to process the data, identify the stock with the highest probability of profit, and display results.

**Project Goals**

- Provide a real-time stock data processing pipeline with Kafka integration.
- Ensure accurate and timely calculation of stock metrics.
- Handle potential data issues and errors effectively.

This project provides a robust framework for real-time stock data analysis and streaming, leveraging modern data processing and streaming technologies.
