import psycopg2
import os
import datetime
import logging
import time
import requests
from psycopg2.extras import execute_values
import pandas as pd
from datetime import datetime

# Set up logging
logging.basicConfig(filename='stock_data_dividend.log', level=logging.INFO)

# NASDAQ API credentials (assumed to be set as environment variables)
FMP_API_KEY = os.environ.get('FMP_KEY')

# Database credentials (assumed to be set as environment variables)
db_name = os.environ.get('QAS_DB_NAME')
db_user = os.environ.get('QAS_DB_USER')
db_password = os.environ.get('QAS_DB_PASSWORD')
db_host = os.environ.get('QAS_DB_HOST')


def get_companies():
    cur.execute("SELECT id, ticker_symbol FROM public.companies")
    return cur.fetchall()

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host)
except Exception as e:
    logging.error(f"Database connection failed: {e}")
    raise

cur = conn.cursor()

def insert_dividend_data(company_id, historical_data):
    for record in historical_data:
        # Extract necessary fields from record
        date = record['date']
        dividend = record['dividend']
        # Insert statement
        cur.execute("""
            INSERT INTO public.market_data (company_id, date, dividend_amount)
            VALUES (%s, %s, %s)
            ON CONFLICT (company_id, date) DO UPDATE
            SET dividend_amount = EXCLUDED.dividend_amount;
        """, (company_id, date, dividend))

companies = get_companies()

def fetch_dividend_data(ticker_symbol):
    response = requests.get(f"https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/{ticker_symbol}?apikey={FMP_API_KEY}")
    if response.status_code == 200:
            return response.json()


for company_id, ticker_symbol in companies:
    historical_data = fetch_dividend_data(ticker_symbol)
    insert_dividend_data(company_id, historical_data['historical'])
    print(ticker_symbol)
    time.sleep(1)
    # Commit after each company's data is processed
    conn.commit()

# Close the connection
cur.close()
conn.close()
