import psycopg2
import os
import datetime
import logging
import time
import requests
from psycopg2.extras import execute_values
import yfinance as yf

# Set up logging
logging.basicConfig(filename='stock_data_download.log', level=logging.INFO)

# Database credentials (assumed to be set as environment variables)
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_host = os.environ.get('DB_HOST')


def get_companies_to_fetch(cur, limit):
    cur.execute("""
        SELECT c.id, c.ticker_symbol
        FROM companies c
        LEFT JOIN company_fetch_status cfs ON c.id = cfs.company_id
        WHERE cfs.last_fetched IS NULL OR cfs.last_fetched < CURRENT_DATE
        LIMIT %s;
    """, (limit,))
    return cur.fetchall()

def update_fetch_status(cur, company_id):
    cur.execute("""
        INSERT INTO company_fetch_status (company_id, last_fetched)
        VALUES (%s, CURRENT_DATE)
        ON CONFLICT (company_id) DO UPDATE SET last_fetched = CURRENT_DATE;
    """, (company_id,))

def download_stock_data(ticker):
    try:
        # Create a Ticker object for the given ticker symbol
        stock = yf.Ticker(ticker)

        # Fetch historical data for the past 5 years
        historical_data = stock.history(period='5y')
        #print(historical_data)
        return historical_data
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

def insert_stock_data(cur, stock_data_tuples):
    try:
        execute_values(cur, """
            INSERT INTO market_data (company_id, date, open_price, close_price, high_price, low_price, volume)
            VALUES %s
            ON CONFLICT (company_id, date) DO NOTHING; 
        """, stock_data_tuples)
        return True
    except psycopg2.IntegrityError as e:
        logging.error(f"IntegrityError while inserting data: {e}")
        print(e)
        return False
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        print(e)
        return False
# Fetch ticker symbols from the companies table

try:
    cur.execute("SELECT id, ticker_symbol FROM companies")
    companies = cur.fetchall()
except Exception as e:
    logging.error(f"Error fetching companies: {e}")
    cur.close()
    conn.close()
    raise


# Fetch companies to fetch
companies_to_fetch = get_companies_to_fetch(cur, 7000)
for company_id, ticker_symbol in companies_to_fetch:

    # Collect all data for the batch in a list of tuples
    stock_data_tuples = []

    print(ticker_symbol)
    stock_data = download_stock_data(ticker_symbol)
    time.sleep(0.25)  # Sleep to handle API rate limiting
    if stock_data is not None:
        # Iterate over each row in the DataFrame
        for index, data in stock_data.iterrows():
            # Convert the index (date) to string format
            date_str = index.strftime('%Y-%m-%d')

            # Append a tuple with the required information
            stock_data_tuples.append((company_id, date_str, data['Open'], data['Close'], data['High'], data['Low'], data['Volume']))

    # Insert all collected data in bulk
    if stock_data_tuples:
        success = insert_stock_data(cur, stock_data_tuples)
        if success:
            conn.commit()
            update_fetch_status(cur, company_id)
            print('Data Inserted Successfully')
        else:
            conn.rollback()

    time.sleep(3.25)  # Ensure 'request_delay' is defined earlier in your code

# Close cursor and connection
cur.close()
conn.close()