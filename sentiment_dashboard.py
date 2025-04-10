import streamlit as st
import psycopg2
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime


# Connect to PostgreSQL
def get_sentiment_counts(stock_ticker, source):
    conn = psycopg2.connect(
        dbname='stock_sentiment',
        user='postgres',
        password='postgres',  # Update this with your password
        host='localhost',
        port='5432'
    )
    cursor = conn.cursor()
    
    query = """
        SELECT sentiment, COUNT(*) 
        FROM stock_sentiment 
        WHERE stock_ticker = %s AND source = %s
        GROUP BY sentiment;
    """
    cursor.execute(query, (stock_ticker, source))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return dict(results)

def get_price_data(stock_ticker):
    conn = psycopg2.connect(
        dbname='stock_sentiment',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )
    query = """
        SELECT price_date, open_price, high_price, low_price, close_price
        FROM stock_prices
        WHERE stock_ticker = %s
        ORDER BY price_date ASC;
    """
    df = pd.read_sql_query(query, conn, params=(stock_ticker,))
    conn.close()
    return df



# Streamlit UI
st.title("📊 Stock Sentiment Dashboard")

# Select stock ticker
stock_ticker = st.selectbox("Choose a stock:", ["TSLA", "NVDA","PLTR"])

# Fetch sentiment for Twitter and NewsAPI sources
newsapi_sentiments = get_sentiment_counts(stock_ticker, "NewsAPI")
stock_prices=get_price_data(stock_ticker)

st.subheader(f"Sentiment from NewsAPI for {stock_ticker}")
if newsapi_sentiments:
    # Increase the size of the pie chart by setting figsize
    extraction_date = datetime.now().strftime('%Y-%m-%d')
    st.write(f"Sentiment Data Extracted on: {extraction_date}")

    
    fig, ax = plt.subplots()  # Adjust the size here (8x8 inches)
    labels = newsapi_sentiments.keys()
    sizes = newsapi_sentiments.values()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')
    st.pyplot(fig)
else:
    st.warning("No sentiment data found for NewsAPI.")

# --- Second Row: Price Chart and Price Data Table ---
col1, col2 = st.columns(2)

# Stock Price Table & Closing Price Line Chart (next to pie chart)
with col1:
    if stock_prices is not None and not stock_prices.empty:  # Check if DataFrame is not empty
        # Display stock price table
        st.subheader(f"Stock Prices for {stock_ticker}")
        st.dataframe(stock_prices)  # Display as a DataFrame
    else:
        st.warning(f"No price data found for {stock_ticker}.")

with col2:
    if stock_prices is not None and not stock_prices.empty:  # Check if DataFrame is not empty
        # Plot closing price line chart
        st.subheader(f"Closing Prices for {stock_ticker}")
        # Increase the size of the line chart by setting figsize
        fig, ax = plt.subplots(figsize=(10, 6))  # Adjust the size here (10x6 inches)
        ax.plot(stock_prices['price_date'], stock_prices['close_price'], marker='o', linestyle='-', color='b', label='Close Price')
        ax.set_xlabel('Date')
        ax.set_ylabel('Close Price')
        ax.set_title(f'{stock_ticker} Close Price Trend')
        ax.grid(True)
        st.pyplot(fig)
    else:
        st.warning(f"No price data found for {stock_ticker}.")