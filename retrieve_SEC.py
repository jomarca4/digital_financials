import sqlite3
import json

import pandas as pd
import sqlite3
import requests
import time
import sys 
import logging 
import datetime
import psycopg2
from sqlalchemy import create_engine, MetaData

# Database credentials
import os
import gc


db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_host = os.environ.get('DB_HOST')
email = os.environ.get('EMAIL')
#create formatter to log timestamp with each message:
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

logging.basicConfig(filename='tracker_db_records.log',level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
#get logging instance

logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
count_printed_loop = 0


print('started')

records_added_1 = 0
records_added = 0
records_added_2 = 0


# Connect to PostgreSQL using the default user 
def create_connection(db_name, db_user, db_password, db_host):
    conn = None
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host
        )
        return conn
    except OperationalError as e:
        print(f"The error '{e}' occurred")

conn = create_connection(db_name, db_user, db_password, db_host)

engine = create_engine('postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}')

cur = conn.cursor()


def add_new_income_statement(conn, entity_name, ticker, cik, location, year, quarter, fs_type, date_fs, currency, unit_of_measurement, value, name_account_item):
    try:
        with conn.cursor() as cur:
            # Begin a transaction
            cur.execute("BEGIN")

            # Check for existing company and get company_id
            cur.execute("SELECT id FROM companies WHERE ticker_symbol = %s", (ticker,))
            company_id = cur.fetchone()[0] if cur.rowcount != 0 else None

            if not company_id:
                # Insert new company if it doesn't exist and get the new company_id
                cur.execute("INSERT INTO companies (name, ticker_symbol, cik, location) VALUES (%s, %s, %s, %s) RETURNING id",
                            (entity_name, ticker, cik, location))
                company_id = cur.fetchone()[0]

            # Check if quarter already exists
            cur.execute(
                "SELECT id FROM quarters WHERE company_id = %s AND year = %s AND quarter_number = %s",
                (company_id, year, quarter))
            quarter_id = cur.fetchone()[0] if cur.rowcount != 0 else None

            if not quarter_id:
                # Insert quarter and financial statement record if they do not exist
                cur.execute("INSERT INTO quarters (year, quarter_number, company_id) VALUES (%s, %s, %s) RETURNING id",
                            (year, quarter, company_id))
                quarter_id = cur.fetchone()[0]

                cur.execute("INSERT INTO financial_statements (type, date, currency, quarter_id) VALUES (%s, %s, %s, %s) RETURNING id",
                            (fs_type, date_fs, currency, quarter_id))
                financial_statement_id = cur.fetchone()[0]

            else:
                # If quarter exists, get the financial_statement_id for that quarter
                cur.execute("SELECT id FROM financial_statements WHERE quarter_id = %s AND type = %s",
                            (quarter_id, fs_type))
                financial_statement_id = cur.fetchone()[0] if cur.rowcount != 0 else None

                if not financial_statement_id:
                    # If financial statement does not exist for the quarter, create it
                    cur.execute("INSERT INTO financial_statements (type, date, currency, quarter_id) VALUES (%s, %s, %s, %s) RETURNING id",
                                (fs_type, date_fs, currency, quarter_id))
                    financial_statement_id = cur.fetchone()[0]

            # Check if financial statement item exists
            cur.execute(
                "SELECT id FROM financial_statement_items WHERE account_label = %s AND financial_statement_id = %s",
                (name_account_item, financial_statement_id))
            if cur.rowcount == 0:
                # Add financial statement item data to the database
                cur.execute('''INSERT INTO financial_statement_items (account_label, value, unit_of_measurement, financial_statement_id) VALUES (%s, %s, %s, %s)''', 
                            (name_account_item, value, unit_of_measurement, financial_statement_id))

            # Commit the transaction
            conn.commit()

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        conn.rollback()


#add_new_statement('NVS')
#exit()

headers = {'User-Agent': email}
#fs_type = 'balance_sheet'
financial_statement_id = 1
#for BS I need an I at the end
#The first element sys.argv[0] is the name of the script itself, and sys.argv[1] is the first command-line argument, which is the value given with running python
time_frame_of_request = str(sys.argv[1])#'CY2022Q1'
# Extract the year substring
year = time_frame_of_request[2:6]
#print(year)
# Extract the quarter substring
try:
    quarter = time_frame_of_request[7:8]
    #print(len(time_frame_of_request))
    if len(time_frame_of_request) == 6:
        quarter = '9'
except:
    print('problem in the quarter transf here')
    exit()

#balance sheet included below
#US_GAAP_ITEMS = pd.read_csv('US_GAAP_TAXONOMY.csv')
US_GAAP_ITEMS = pd.read_excel('GAAP_Taxonomy_2022.xlsx',sheet_name='Calculation Link',engine='openpyxl') #balance_sheet|cash_flow|income_statement|other_comprehensive_income
US_GAAP_ITEMS = US_GAAP_ITEMS[(US_GAAP_ITEMS['financial_statement_type'].str.contains('balance_sheet|cash_flow|income_statement|other_comprehensive_income'))]
CIK_ENTITY_MAPPING = pd.read_csv('CIK_COMPANY_MAPPING.csv')

#print(US_GAAP_ITEMS)
print(len(US_GAAP_ITEMS['name']))

# Assume BATCH_SIZE is the number of records you process in each batch
BATCH_SIZE = 100  # Adjust this based on your memory constraints

# Function to process a batch of items
def process_batch(batch_items, conn):
    tag_counter = 0
    for item,item1 in zip(US_GAAP_ITEMS['name'],US_GAAP_ITEMS['financial_statement_type']):
        #clear variables to gain memory:
        #print(item,item1) #assets, balance shetet
        fs_type = item1
        time.sleep(0.25)
        print(item) #SalesTypeLeaseNetInvestmentInLeaseExcludingAccruedInterestAfterAllowanceForCreditLossCurrent
        #STARTS HERE
        name_account_item = item #'CashAndCashEquivalentsAtCarryingValue'
        #print(tag_counter)

        url = f'https://data.sec.gov/api/xbrl/frames/us-gaap/{name_account_item}/USD/{time_frame_of_request}.json'
        response = requests.get(url,headers=headers)
        #print(response)
        try:
            data = json.loads(response.text)
        except:
            print('name tag does not exist ',name_account_item)
            print('tag counter is ', tag_counter)
            #continue with next item in the loop
            tag_counter = tag_counter + 1
            continue
        #csv file contains the entity to cik mapping
        frames = data['data']
        currency = data['uom']
        unit_of_measurement = data['uom']
        tag_counter = tag_counter + 1
        value = None
        count = 0
        #print(item)
        
        for item in frames:
            value = None
            entity_name = None
            cik = None
            date_fs = None
            location = None
            ticker = None
            value = item['val']
            entity_name = item['entityName']
            cik = item['cik']
            date_fs = item['end']
            location = item['loc']
            # Find the row with the matching CIK value to map to ENTITY NAME
            ticker = CIK_ENTITY_MAPPING.loc[CIK_ENTITY_MAPPING['CIK'] == cik]
            try:
                ticker = ticker['COMPANY'].values[0]
                ticker = ticker.upper()
            except:
                ticker = 'Not available'
            #add leadings zeros since I need to have a 10 digit variable
            cik = str(cik).zfill(10)

            add_new_income_statement(conn, entity_name, ticker, cik, location, year, quarter, fs_type, date_fs, currency, unit_of_measurement, value, name_account_item)
    print(records_added, records_added_2,records_added_1)

    del batch_items
    gc.collect()
        
# Main processing loop with batch processing
for i in range(0, len(US_GAAP_ITEMS), BATCH_SIZE):
    batch = US_GAAP_ITEMS.iloc[i:i + BATCH_SIZE]
    process_batch(zip(batch['name'], batch['financial_statement_type']), conn)
    del batch
    gc.collect()
        # Clear memory


conn.close()
logging.shutdown()
