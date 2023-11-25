import psycopg2
import os
import datetime
import logging
import time
import requests
from psycopg2.extras import execute_values

# Set up logging
logging.basicConfig(filename='stock_data_download.log', level=logging.INFO)

# NASDAQ API credentials (assumed to be set as environment variables)
FMP_API_KEY = os.environ.get('FMP_KEY')

# Database credentials (assumed to be set as environment variables)
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_host = os.environ.get('DB_HOST')

# Set date range for the past year
end_date = datetime.datetime.today()
start_date = end_date - datetime.timedelta(days=1500)

# Format dates to 'YYYY-MM-DD' format for Financial Modeling Prep API
end_date_str = end_date.strftime('%Y-%m-%d')
start_date_str = start_date.strftime('%Y-%m-%d')

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
        # Construct the API URL for Financial Modeling Prep
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?from={start_date_str}&to={end_date_str}&apikey={FMP_API_KEY}"
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
        data = response.json()
        
        
        return data['historical']  # Adjust based on Financial Modeling Prep's response structure
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

# Batch processing variables
batch_size = 2  # Number of companies to process in each batch
request_delay = 1  # Delay between each batch in seconds
 # Fetch the next batch of companies
companies_to_fetch = get_companies_to_fetch(cur, 250)
# Download and store stock data in batches
# Download and store stock data in batches
for company_id, ticker_symbol in companies_to_fetch:

    # Collect all data for the batch in a list of tuples
    stock_data_tuples = []

    print(ticker_symbol)
    stock_data = download_stock_data(ticker_symbol)
    time.sleep(1)  # Sleep to handle API rate limiting

    if stock_data is not None:
        for data in stock_data:
            stock_data_tuples.append((company_id, data['date'], data['open'], data['close'], data['high'], data['low'], data['volume']))
    #print(stock_data_tuples)
    # Insert all collected data in bulk
    if stock_data_tuples:
        success = insert_stock_data(cur, stock_data_tuples)
                #print(success)
        if success:
            conn.commit()
            update_fetch_status(cur, company_id)
            print('ok')

            #print('ok')
        else:
            conn.rollback()

    time.sleep(request_delay)

# Close cursor and connection
cur.close()
conn.close()