from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import tweepy
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import psycopg2
import time
import yfinance as yf


# Define your PostgreSQL connection details
POSTGRES_CONN_ID = "postgres_local"  # Use Airflow connection ID

TWITTER_TOKEN='AAAAAAAAAAAAAAAAAAAAAPYE0gEAAAAA8H5yL644u34QDVfRYT5ZzU3Ftpk%3DeC0IHm7fJEXqAwNl0bKevfl7LOdcY1af5FTl4RgQ2COoSkFS1Q'
NEWS_API_KEY = '810089da5bf549d2b2bc87f48e982868'

# Initialize VADER sentiment analyzer
analyzer = SentimentIntensityAnalyzer()


# Function to fetch news articles
def fetch_news(stock_ticker, count=100):
    url = f'https://newsapi.org/v2/everything?q={stock_ticker}&apiKey={NEWS_API_KEY}&pageSize={count}'
    response = requests.get(url)
    articles = response.json()
    print(f"Found {len(articles['articles'])} news articles.")

    return [article['title'] for article in articles['articles']]


# Example function to fetch tweets
def fetch_tweets(stock_tickers, count=10):
    headers = {
        "Authorization": f"Bearer {TWITTER_TOKEN}"
    }

    for stock_ticker in stock_tickers:
        query = f"{stock_ticker} stock OR {stock_ticker} earnings OR {stock_ticker} buy OR {stock_ticker} sell OR {stock_ticker} hold -is:retweet lang:en"
        url = f"https://api.twitter.com/2/tweets/search/recent?query={query}&max_results={count}&tweet.fields=text"
        
        tweets = response.json().get('data', [])
        return [tweet['text'] for tweet in tweets]





# Sentiment analysis function
def analyze_sentiment(text):
    sentiment_score = analyzer.polarity_scores(text)
    if sentiment_score['compound'] >= 0.05:
        return 'Positive'
    elif sentiment_score['compound'] <= -0.05:
        return 'Negative'
    else:
        return 'Neutral'

# Function to load sentiment data into PostgreSQL
def load_to_postgres(stock_ticker, sentiment, source, content):
    # Connect to PostgreSQL and insert data

    conn = psycopg2.connect(dbname='stock_sentiment', user='postgres', password='postgres', host='localhost')
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO stock_sentiment (stock_ticker, sentiment, source, content) 
    VALUES (%s, %s, %s, %s);
    """
    cursor.execute(insert_query, (stock_ticker, sentiment, source, content))
    conn.commit()
    cursor.close()
    conn.close()

def fetch_and_store_stock_prices(stock_ticker):
    # Fetch past 5 days, then slice to last 3 rows
    stock = yf.Ticker(stock_ticker)
    hist = stock.history(period="5d").tail(3)

    conn = psycopg2.connect(dbname='stock_sentiment', user='postgres', password='postgres', host='localhost')
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO stock_prices (stock_ticker, price_date, open_price, high_price, low_price, close_price)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_ticker, price_date) DO NOTHING;
    """

    for date, row in hist.iterrows():
        cursor.execute(insert_query, (
            stock_ticker,
            date.date(),  # convert Timestamp to just date
            float(row['Open']),
            float(row['High']),
            float(row['Low']),
            float(row['Close'])
        ))

    conn.commit()
    cursor.close()
    conn.close()

# Define ETL tasks
def extract_and_transform(**kwargs):
    stock_ticker = kwargs['stock_ticker']
    
    # Fetch tweets and news
   # tweets = fetch_tweets(stock_ticker)
    fetch_and_store_stock_prices(stock_ticker)

    news_articles = fetch_news(stock_ticker)

    """
    # Analyze sentiment for tweets
    for tweet in tweets:
        sentiment = analyze_sentiment(tweet)
        load_to_postgres(stock_ticker, sentiment, 'Twitter', tweet) """
    
    # Analyze sentiment for news articles
    for article in news_articles:
        sentiment = analyze_sentiment(article)
        load_to_postgres(stock_ticker, sentiment, 'NewsAPI', article)



   
def count_sentiments(**kwargs):
    stock_tickers = kwargs.get('stock_tickers', [])

    conn = psycopg2.connect(
        dbname='stock_sentiment',
        user='postgres',
        password='postgres',
        host='localhost'  # or your actual DB host
    )
    cursor = conn.cursor()

    for stock_ticker in stock_tickers:
        query = """
            SELECT sentiment, COUNT(*) 
            FROM stock_sentiment 
            WHERE stock_ticker = %s 
            GROUP BY sentiment;
        """
        cursor.execute(query, (stock_ticker,))
        results = cursor.fetchall()

        print(f"\nSentiment counts for {stock_ticker}:")
        for sentiment, count in results:
            print(f"{sentiment}: {count}")

    cursor.close()
    conn.close()
    
# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 9),
}

dag = DAG(
    'stock_sentiment_etl',
    default_args=default_args,
    description='ETL pipeline to analyze stock sentiment',
    schedule_interval=timedelta(days=1),  # Run once a day
    catchup=False,
)

# Task to extract and transform data
extract_and_transform_stock1 = PythonOperator(
    task_id='extract_and_transform_tsla',
    python_callable=extract_and_transform,
    op_kwargs={'stock_ticker': 'TSLA'},  # You can change the stock symbol here
    dag=dag,
)

extract_and_transform_stock2 = PythonOperator(
    task_id='extract_and_transform_nvda',
    python_callable=extract_and_transform,
    op_kwargs={'stock_ticker': 'NVDA'},  # You can change the stock symbol here
    dag=dag,
)
create_table_task = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql='''
    CREATE TABLE IF NOT EXISTS stock_sentiment (
        id SERIAL PRIMARY KEY,
        stock_ticker VARCHAR(10),
        sentiment VARCHAR(10),
        source VARCHAR(50),
        content TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
   
    ''', 
    dag=dag,
)
create_stock_prices_table_task = PostgresOperator(
    task_id='create_stock_prices_table',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql='''
    CREATE TABLE IF NOT EXISTS stock_prices (
        id SERIAL PRIMARY KEY,
        stock_ticker VARCHAR(10),
        price_date DATE,
        open_price FLOAT,
        high_price FLOAT,
        low_price FLOAT,
        close_price FLOAT,
        UNIQUE (stock_ticker, price_date)
    );
    ''', 
    dag=dag,
)
count_sentiments_task = PythonOperator(
    task_id='count_sentiments',
    python_callable=count_sentiments,
    op_kwargs={'stock_tickers': ['TSLA', 'NVDA']},
    dag=dag,
)

# Set the task dependencies
create_table_task>>create_stock_prices_table_task>> [extract_and_transform_stock1, extract_and_transform_stock2]
[extract_and_transform_stock1, extract_and_transform_stock2] >> count_sentiments_task

