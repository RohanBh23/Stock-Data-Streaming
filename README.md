<<<<<<< HEAD
# Trading Engine

<img width="544" alt="image" src="https://github.com/user-attachments/assets/8d4ae8dc-e5cc-40cd-96cf-e49eedff32f7">



### Real-Time Stock Data Stream Processing leveraging Kafka

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
=======
Overview
========

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
    - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://www.astronomer.io/docs/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.
>>>>>>> master
