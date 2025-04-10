# Stock Sentiment Analysis & Visualization
This project analyzes stock-related news articles, performs sentiment analysis, fetches stock prices, stores the results in a PostgreSQL database, and provides an interactive data visualization dashboard using Streamlit. The project also leverages Airflow to automate the ETL pipeline.


Table of Contents
1. [Overview](#overview)
2. [Technologies](#technologies)
4. [Setup](#setup)
5. [ETL](#etl)
6. [Visualization](#visualization)


## Overview
This project involves multiple components that interact together:

- News Article Sentiment Analysis: Fetches news articles about a specific stock using an external API (NewsAPI), performs sentiment analysis on these articles, and categorizes them into positive, negative, or neutral sentiments.

- PostgreSQL Database: Sentiment data and stock price information are stored in a PostgreSQL database for easy retrieval and further analysis.

- Stock Market Price: Fetches stock market prices using the Yahoo Finance API.

- Streamlit Dashboard: An interactive dashboard built using Streamlit visualizes sentiment analysis results and stock price trends.

- ETL Pipeline with Apache Airflow: Airflow orchestrates the extraction, transformation, and loading (ETL) process, fetching news articles, performing sentiment analysis, and storing the results in the database.

## Technologies
- NewsAPI: Used to fetch the latest news articles related to specific stocks.

- Sentiment Analysis: Sentiment of articles is analyzed using the VADER sentiment analysis model.

- Yahoo Finance API: Used to fetch stock market prices.

- PostgreSQL: A relational database to store news sentiment and stock price data.

- Streamlit: A Python framework for building interactive web dashboards.

- Apache Airflow: Used to automate the ETL pipeline that fetches, analyzes, and stores data.

- Python: Main programming language, with libraries including requests, psycopg2, matplotlib, pandas, streamlit, yfinance, and vaderSentiment.

## Setup
### Prerequisites
Make sure you have the following installed on your machine:

1. Python 3.x: Install it from the official Python website if not already installed.

2. PostgreSQL: Install PostgreSQL on your machine or set up a remote PostgreSQL instance. You can download it from the PostgreSQL website.

3. pip: Python's package installer. Ensure you have it installed to manage dependencies.

4. Airflow: Install Apache Airflow to automate the ETL pipeline.

### Install Dependencies
Clone this repository and navigate to the project directory:
```
git clone https://github.com/yourusername/stock-sentiment-analysis.git
cd stock-sentiment-analysis
```

You can install all dependencies using pip:

```
pip install apache-airflow apache-airflow-providers-postgres requests vaderSentiment psycopg2 yfinance streamlit
```


### Requirements
To run the project successfully, you'll need:

- NewsAPI API Key: This key is required to fetch news articles related to a specific stock ticker. You can obtain it by signing up on the NewsAPI website.

Once you have the key, you need to set it as an environment variable or update the code with the key directly in the NEWS_API_KEY variable.








## ETL
The ETL pipeline is defined using Apache Airflow, which orchestrates the following tasks:

1. Extract News Articles and Stock Prices:

- News articles are fetched using the NewsAPI for a given stock ticker.

- Stock prices for the past 5 days are fetched using the Yahoo Finance API.

2. Transform Data:

- Sentiment analysis is performed on the news articles using VADER sentiment analysis. The results are classified as Positive, Negative, or Neutral.

3. Load Data to PostgreSQL:

- Sentiment data and stock prices are loaded into the PostgreSQL database.

- The sentiment data is stored in the stock_sentiment table, and the stock price data is stored in the stock_prices table.

4. Airflow DAG:

- The pipeline is managed by Airflow, and tasks are scheduled to run daily. The DAG is defined with dependencies to ensure that data is fetched, transformed, and loaded correctly.

## Visualization
Start Streamlit Dashboard: After the ETL process loads data into the PostgreSQL database, you can run the Streamlit dashboard to visualize the data:
```
streamlit run sentiment_dashboard.py
```
