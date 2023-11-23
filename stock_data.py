import psycopg2
from alpaca_trade_api.rest import REST, TimeFrame
import os
import datetime
import logging
import time

# Set up logging
logging.basicConfig(filename='stock_data_download.log', level=logging.INFO)

# Alpaca API credentials
API_KEY = os.environ.get('ALPACA_API_KEY')
API_SECRET = os.environ.get('ALPACA_SECRET_KEY')
BASE_URL = "https://paper-api.alpaca.markets"  # Update if necessary

# Initialize Alpaca API
alpaca_api = REST(API_KEY, API_SECRET, base_url=BASE_URL)

# Database credentials (assumed to be set as environment variables)
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_host = os.environ.get('DB_HOST')

# Initialize Alpaca API
alpaca_api = REST(API_KEY, API_SECRET, base_url=BASE_URL)

# Set date range for the past year
end_date = datetime.datetime.today()
start_date = end_date - datetime.timedelta(days=365)

# Format dates in 'YYYY-MM-DD' format for the API call
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

def download_stock_data(ticker):
    try:
        data = alpaca_api.get_bars(ticker, TimeFrame.Day, start=start_date_str, end=end_date_str).df
        return data
    except Exception as e:
        logging.error(f"Failed to download data for {ticker}. Error: {e}")
        print(e)
        return None

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host)
except Exception as e:
    logging.error(f"Database connection failed: {e}")
    raise

cur = conn.cursor()

# Fetch ticker symbols from the companies table
try:
    cur.execute("SELECT id, ticker_symbol FROM companies")
    companies = cur.fetchall()
except Exception as e:
    logging.error(f"Error fetching companies: {e}")
    cur.close()
    conn.close()
    raise

# Setting up a date range for the past year
end_date = datetime.datetime.today()
start_date = end_date - datetime.timedelta(days=300)

# Batch processing variables
batch_size = 30  # Number of companies to process in each batch
request_delay = 10  # Delay between each batch in seconds

# Download and store stock data in batches
for i in range(0, len(companies), batch_size):
    batch_companies = companies[i:i+batch_size]

    for company_id, ticker_symbol in batch_companies:
        print(ticker_symbol)
        ticker_symbol = 'AAPL'
        stock_data = download_stock_data(ticker_symbol)
        print(stock_data)
        exit()
        if stock_data is not None:
            try:
                # Insert data into market_data table
                for index, row in stock_data.iterrows():
                    cur.execute("""
                        INSERT INTO market_data (company_id, date, open_price, close_price, high_price, low_price, volume) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (company_id, index.date(), row['open'], row['close'], row['high'], row['low'], row['volume']))
            except Exception as e:
                logging.error(f"Error inserting data for {ticker_symbol}: {e}")
    
    # Commit after each batch and wait before proceeding to the next batch
    conn.commit()
    time.sleep(request_delay)

# Close cursor and connection
cur.close()
conn.close()