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
# print the schema https://fasb.org/Page/PageContent?PageId=/xbrl/2022financial.html #download from here Excel taxonomy (zip cannot be open in mac only in ophone)
#http://xbrlview.fasb.org/yeti/resources/yeti-gwt/Yeti.jsp#tax~(id~174*v~6430)!con~(id~4380877)!net~(a~3474*l~832)!lang~(code~en-us)!path~(g~99043*p~0_0_2_0_0_0)!rg~(rg~32*p~12)
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

# Replace these values with your own database credentials and table name

# Create a connection string to connect to the database
#db_string = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'

# Create an engine that will manage connections to the database
engine = create_engine('postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}')

# Create a cursor object
cur = conn.cursor()
#cur.execute("ROLLBACK")

def update_taxonomy():
    US_GAAP_ITEMS_TAXONOMY = pd.read_excel('GAAP_Taxonomy_2022.xlsx',sheet_name='Calculation Link',engine='openpyxl')
    US_GAAP_ITEMS_TAXONOMY = US_GAAP_ITEMS_TAXONOMY[['definition','name','label','depth','ranking','parent','financial_statement_type']]
    # Define the data types for each column in the DataFrame
    
    # Insert the data into the new table
    US_GAAP_ITEMS_TAXONOMY.to_sql(name='taxonomy', con=engine, if_exists='append', index=False)
    conn.commit()
    conn.close()
#update_taxonomy()


#exit()
def add_new_income_statement():
    #make variables global to have access inside function
    global currency,unit_of_measurement,value,entity_name,cik,date_fs,location,ticker,year,quarter, financial_statement_id,fs_type,name_account_item, records_added,records_added_1,records_added_2, tag_counter, count_printed_loop
    time.sleep(0.05)
    #print(entity_name)

    # First, check if Apple is already in the companies table
    #company_exists_check = conn.execute("SELECT * FROM companies WHERE name = ?", (entity_name,)).fetchone() #SQLITE3
    try:
        cur.execute("BEGIN")

        # Check for existing company by ticker
        cur.execute("SELECT id FROM companies WHERE ticker_symbol = %s", (str(ticker),))
        company_exists_check = cur.fetchone()

        if not company_exists_check:
            # Insert new company if it doesn't exist
            cur.execute("INSERT INTO companies (name, ticker_symbol, cik, location) VALUES (%s, %s, %s, %s) RETURNING id",
                        (entity_name, ticker, cik, location))
            company_id = cur.fetchone()[0]
            records_added_1 += 1
        else:
            company_id = company_exists_check[0]

        # second, check if quarter already exists
    #quarter_exist_check = conn.execute(f"SELECT * FROM quarters WHERE company_id = '{company_id}' AND year = {year} AND quarter_number = '{quarter}' ").fetchone()

        quarter_exist_check = cur.execute(
        "SELECT q.*, c.name \
        FROM quarters q \
        JOIN companies c ON q.company_id = c.id \
        WHERE q.company_id = %s AND q.year = %s AND q.quarter_number = %s",
        (company_id, year, quarter))
        quarter_exist_check = cur.fetchone()
        quarter_exist_check = None
    
        #print('step2')
        if not quarter_exist_check:  #if none exists prints none
            #print(f"No quarter data found for company {company_id}, year {year}, and quarter {quarter}")
            #insert quarter and financial statement record since they do not exist:
            result = None
            result = cur.execute("INSERT INTO quarters (year, quarter_number, company_id) VALUES (%s, %s, %s) RETURNING id",
                        (year, quarter, company_id))
            result = cur.fetchone()  # Consume the result of the INSERT statement
            cur.execute("SELECT lastval()")
            quarter_id = cur.fetchone()[0]
            #print(quarter_id)
            # Insert a new financial statement record for the balance sheet as of the end of the quarter (you can adjust the date as needed)
            result = None
            #print('step3')
            result = cur.execute("INSERT INTO financial_statements (type, date, currency, quarter_id) VALUES (%s, %s, %s, %s) RETURNING id",
                        (fs_type, date_fs, currency, quarter_id))
            cur.fetchone()
            cur.execute("SELECT lastval()")
            financial_statement_id = cur.fetchone()[0]
            #print(financial_statement_id)
            
            cur.execute('''INSERT INTO financial_statement_items (account_label, value, unit_of_measurement, financial_statement_id) VALUES (%s, %s, %s, %s)''', (name_account_item, value, unit_of_measurement, financial_statement_id))
            #tag_counter = tag_counter + 1
            records_added_2 = records_added_2 + 1
            conn.commit()
        else:
            #if quarter exists, I do not want to add quarter again, instead I add only account item if it does not exist
            quarter_id = quarter_exist_check[0]
            existing_quarter = quarter_exist_check[2]
            existing_year = quarter_exist_check[1]
            company_name = quarter_exist_check[4]
            # Check if account_label exists in financial_statement_items table
            account_label_exists = cur.execute(
            "SELECT * \
            FROM financial_statement_items \
            JOIN financial_statements ON financial_statement_items.financial_statement_id = financial_statements.id \
            JOIN quarters ON financial_statements.quarter_id = quarters.id \
            WHERE financial_statement_items.account_label = %s AND quarters.year = %s AND quarters.quarter_number = %s AND quarters.company_id = %s",
            (name_account_item, existing_year, existing_quarter, company_id))
            try:
                account_label_exists = cur.fetchone()
            except:
                account_label_exists = None
            #print('step 7')
            #if account_label exists,
            if (account_label_exists is not None) :
                #print(f"Financial statement item with name {name_account_item} already exists for company {company_name}, year {year}, and quarter {quarter}")
                #print('not added')
                variable_dummy = 'ok'
            else:
                #print('added')
            # Add financial statement item data to the database
            #insert to quarter:
                quarter_id = cur.execute("INSERT INTO quarters (year, quarter_number, company_id) VALUES (%s, %s, %s) RETURNING id",
                                (year, quarter, company_id))
                cur.fetchone()  # Consume the result of the INSERT statement
                cur.execute("SELECT lastval()")
                quarter_id = cur.fetchone()[0]
                #print('step 8')
            # Insert a new financial statement record for the balance sheet as of the end of the quarter (you can adjust the date as needed)
                financial_statement_id = cur.execute("INSERT INTO financial_statements (type, date, currency, quarter_id) VALUES (%s, %s, %s, %s) RETURNING id",
                                    (fs_type, date_fs, currency, quarter_id))
                cur.fetchone()  # Consume the result of the INSERT statement
                cur.execute("SELECT lastval()")
                financial_statement_id = cur.fetchone()[0]
                #print('step 9')        
                try:
                    cur.execute('''INSERT INTO financial_statement_items (account_label, value, unit_of_measurement, financial_statement_id) VALUES (%s, %s, %s, %s)''', (name_account_item, value, unit_of_measurement, financial_statement_id))
                except:
                    print('this record gave an error')
                #print('company and record added')
                records_added = records_added + 1
                count_printed_loop = count_printed_loop + 1
                if count_printed_loop > 50:
                    logger.info(f'Record number {tag_counter} entered for {name_account_item} for {entity_name} for year {year} and quarter {quarter}')
                    logger.info(f'record number {records_added} has been added')
                    count_printed_loop = 0
                else:
                    pass
                conn.commit()

        conn.commit()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        cur.execute("ROLLBACK")

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
# Extract the quarter substring
try:
    quarter = time_frame_of_request[7:8]
except:
    quarter = 'Full Year'
#balance sheet included below
#US_GAAP_ITEMS = pd.read_csv('US_GAAP_TAXONOMY.csv')
US_GAAP_ITEMS = pd.read_excel('GAAP_Taxonomy_2022.xlsx',sheet_name='Calculation Link',engine='openpyxl') #balance_sheet|cash_flow|income_statement|other_comprehensive_income
US_GAAP_ITEMS = US_GAAP_ITEMS[(US_GAAP_ITEMS['financial_statement_type'].str.contains('balance_sheet|cash_flow|income_statement|other_comprehensive_income'))]
CIK_ENTITY_MAPPING = pd.read_csv('CIK_COMPANY_MAPPING.csv')

#print(US_GAAP_ITEMS)
print(len(US_GAAP_ITEMS['name']))
tag_counter = 0
for item,item1 in zip(US_GAAP_ITEMS['name'],US_GAAP_ITEMS['financial_statement_type']):
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

        add_new_income_statement()

print(records_added, records_added_2,records_added_1)
conn.close()
logging.shutdown()
