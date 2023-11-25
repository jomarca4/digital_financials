import psycopg2
import os
import datetime
import logging
import time
import requests

# Set up logging
logging.basicConfig(filename='stock_data_download.log', level=logging.INFO)

# NASDAQ API credentials (assumed to be set as environment variables)
NASDAQ_API_KEY = os.environ.get('NASDAQ_API_KEY')

# Database credentials (assumed to be set as environment variables)
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_host = os.environ.get('DB_HOST')

# Set date range for the past year
end_date = datetime.datetime.today()
start_date = end_date - datetime.timedelta(days=365)

def download_stock_data(ticker):
    try:
        # Construct the API URL for NASDAQ
        #url = f"https://data.nasdaq.com/api/v3/datasets/WIKI/{ticker}.json?api_key={NASDAQ_API_KEY}&start_date={start_date.strftime('%Y-%m-%d')}&end_date={end_date.strftime('%Y-%m-%d')}"
        url = f"https://data.nasdaq.com/api/v3/datatables/WIKI/PRICES.json?date.gte={start_date.strftime('yyyymmdd')}&date.lt={end_date.strftime('yyyymmdd')}&ticker={ticker}&api_key={NASDAQ_API_KEY}"
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
        data = response.json()
        print(data)
        exit()
        return data  # You might need to adjust this based on the actual structure of NASDAQ's response
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

# Batch processing variables
batch_size = 100  # Number of companies to process in each batch
request_delay = 1  # Delay between each batch in seconds

# Download and store stock data in batches
for i in range(0, len(companies), batch_size):
    batch_companies = companies[i:i+batch_size]

    for company_id, ticker_symbol in batch_companies:
        print(ticker_symbol)
        stock_data = download_stock_data(ticker_symbol)
        print(stock_data)
        if stock_data is not None:
            for date, data in stock_data['datatable']['data']:
                print(date,data)
                exit()
                try:
                    cur.execute("""
                        INSERT INTO market_data (company_id, date, open_price, close_price, high_price, low_price, volume) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (company_id, date, data['open'], data['close'], data['high'], data['low'], data['volume']))
                except IntegrityError:
                    conn.rollback()  # Rollback the current transaction
                    # Optionally, update the existing record or log the error
                    logging.warning(f"Duplicate entry for {ticker_symbol} on {date}. Skipping insertion.")
                    # Continue with the next iteration
                    continue
                except Exception as e:
                    logging.error(f"Error inserting data for {ticker_symbol}: {e}")
                    continue  # Skip to the next iteration on any other error

    # Commit after each batch and wait before proceeding to the next batch
    conn.commit()
    time.sleep(request_delay)

# Close cursor and connection
cur.close()
conn.close()