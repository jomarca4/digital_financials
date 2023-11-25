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



# Connect to the database
conn = create_connection(db_name, db_user, db_password, db_host)
# Create a cursor object
cur = conn.cursor()

def add_new_table():


    # Load the Excel file
    df = pd.read_excel('GAAP_Taxonomy_2022.xlsx', sheet_name='Calculation Link')

    # Connect to your database

    # Create the SQL table if it doesn't exist
    cur.execute('''
    CREATE TABLE IF NOT EXISTS financial_statement_labels (
        id SERIAL PRIMARY KEY,
        name TEXT,
        label TEXT,
        depth INTEGER,
        financial_statement_type TEXT
    )
    ''')
    conn.commit()

    # Prepare the data for insertion
    # We're selecting only the columns we need
    data_to_insert = df[['name', 'label', 'depth', 'financial_statement_type']]

    # Insert the data
    # psycopg2.extras.execute_batch can be used for efficient bulk inserts
    from psycopg2.extras import execute_batch

    query = """
    INSERT INTO financial_statement_labels (name, label, depth, financial_statement_type) 
    VALUES (%s, %s, %s, %s)
    """

    # Convert DataFrame to list of tuples
    data_tuples = list(data_to_insert.itertuples(index=False, name=None))

    # Execute the query
    execute_batch(cur, query, data_tuples)
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()


add_new_table()
exit()
def alter_table():
        #below is the schema of the database

    # create companies table
    cur.execute('''
   ALTER TABLE market_data
ADD CONSTRAINT unique_company_date UNIQUE (company_id, date);
    ''')
alter_table()
exit()
def delete_duplicates_in_table():
        #below is the schema of the database

    # create companies table
    conn.execute('''
    DELETE FROM financial_statement_items
    WHERE id NOT IN (
  SELECT MIN(id) 
  FROM financial_statement_items 
  GROUP BY name, value, unit_of_measurement
    );
    ''')

def create_new_table():
    # create companies table
    conn.execute('''CREATE TABLE taxonomy (
                 id INTEGER PRIMARY KEY,
                 definition TEXT NOT NULL,
                 name TEXT NOT NULL,
                 label TEXT NOT NULL,
                 depth INTEGER,
                 ranking INTEGER,
                 parent TEXT,
                 financial_statement_type TEXT
               )''')

    conn.commit()
    conn.close()

#create_new_table()

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
def alter_table():
# Alter companies table to add "created_at" field
    cur.execute('''
        ALTER TABLE companies
        ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ''')

    # Alter quarters table to add "created_at" field
    cur.execute('''
        ALTER TABLE quarters
        ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ''')

    # Alter financial_statements table to add "created_at" field
    cur.execute('''
        ALTER TABLE financial_statements
        ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ''')

    # Alter financial_statement_items table to add "created_at" field
    cur.execute('''
        ALTER TABLE financial_statement_items
        ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ''')

    conn.commit()
    cur.close()
    conn.close()
alter_table()
exit()
def create_table():
    #below is the schema of the database
    #conn = sqlite3.connect('financial_statements_SEC_EDGAR.db')

    # create companies table
    cur.execute('''CREATE TABLE companies (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        ticker_symbol TEXT NOT NULL,
                        cik INTEGER NOT NULL,
                        location TEXT NOT NULL
                    )''')

    # create quarters table
    cur.execute('''CREATE TABLE quarters (
                        id SERIAL PRIMARY KEY,
                        year INTEGER NOT NULL,
                        quarter_number TEXT NOT NULL,
                        company_id INTEGER NOT NULL,
                        FOREIGN KEY (company_id) REFERENCES companies (id)
                    )''')

    # create financial statements table
    cur.execute('''CREATE TABLE financial_statements (
                        id SERIAL PRIMARY KEY,
                        type TEXT NOT NULL,
                        date DATE NOT NULL,
                        currency TEXT NOT NULL,
                        quarter_id INTEGER NOT NULL,
                        FOREIGN KEY (quarter_id) REFERENCES quarters (id)
                    )''')

    # create financial statement items table
    cur.execute('''CREATE TABLE financial_statement_items (
                        id SERIAL PRIMARY KEY,
                        account_label TEXT NOT NULL,
                        value REAL NOT NULL,
                        unit_of_measurement TEXT NOT NULL,
                        financial_statement_id INTEGER NOT NULL,
                        FOREIGN KEY (financial_statement_id) REFERENCES financial_statements (id)
                    )''')

    conn.commit()
    cur.close()
    conn.close()


#create_table()
#exit()
def add_new_income_statement():
    #make variables global to have access inside function
    global currency,unit_of_measurement,value,entity_name,cik,date_fs,location,ticker,year,quarter, financial_statement_id,fs_type,name_account_item, records_added,records_added_1,records_added_2, tag_counter, count_printed_loop
    time.sleep(0.05)
    
    # First, check if Apple is already in the companies table
    #company_exists_check = conn.execute("SELECT * FROM companies WHERE name = ?", (entity_name,)).fetchone() #SQLITE3
    try:
        #cur.execute always return none
        company_exists_check = cur.execute("SELECT * FROM companies WHERE name = %s", (str(entity_name),))
        company_exists_check = cur.fetchone() #POSTGRESQL
        #print(company_exists_check)
    except:
        company_exists_check = None
    if not company_exists_check:
        # If Apple is not in the companies table, insert it as a new record
        #conn.execute("INSERT INTO companies (name, ticker_symbol,cik, location) VALUES (?, ?, ?, ?)",
        #            (entity_name, ticker,cik, location)) #SQLITE3
        result = None
        #print('no company exists')
        #print(entity_name,ticker,cik,location)

        result = cur.execute("INSERT INTO companies (name, ticker_symbol, cik, location) VALUES (%s, %s, %s, %s) RETURNING id",
                (entity_name, ticker, cik, location)) #returning ID returns ID         # Get the ID of the newly inserted company record
                        #conn.commit()
        cur.fetchone()
        cur.execute("SELECT lastval()")
        company_id = cur.fetchone()[0]
            #print(company_id)
        records_added_1 = records_added_1 + 1
            #tag_counter = tag_counter + 1
        #except psycopg2.Error as e:
            # Rollback the transaction in case of any exception
        #    conn.rollback()
         #   print("Error:", e)


    else:
        # If Apple is already in the companies table, use its ID
        company_id = company_exists_check[0]
        #print('try here ',company_id)
        #print(company_id)

        # second, check if quarter already exists
    #quarter_exist_check = conn.execute(f"SELECT * FROM quarters WHERE company_id = '{company_id}' AND year = {year} AND quarter_number = '{quarter}' ").fetchone()
    try:
        quarter_exist_check = cur.execute(
        "SELECT q.*, c.name \
        FROM quarters q \
        JOIN companies c ON q.company_id = c.id \
        WHERE q.company_id = %s AND q.year = %s AND q.quarter_number = %s",
        (company_id, year, quarter))
        quarter_exist_check = cur.fetchone()
        #print(quarter_exist_check)
        #print('does it work?')
    except:
        quarter_exist_check = None
    #print(quarter_exist_check)
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
        #print('company and record added')
        #tag_counter = tag_counter + 1
        #print('step 4')
        records_added_2 = records_added_2 + 1
        conn.commit()
    else:
        #if quarter exists, I do not want to add quarter again, instead I add only account item if it does not exist
        quarter_id = quarter_exist_check[0]
        existing_quarter = quarter_exist_check[2]
        existing_year = quarter_exist_check[1]
        company_name = quarter_exist_check[4]
        #print(quarter_exist_check, quarter_id,company_name)
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
        #print(quarter_id,name_account_item,company_id,account_label_exists)
        #if account_label exists,
        if (account_label_exists is not None) :
            #print(f"Financial statement item with name {name_account_item} already exists for company {company_name}, year {year}, and quarter {quarter}")
            #print('not added')
            variable_dummy = 'ok'
        else:
            #print('added')
        # Add financial statement item data to the database
            #print(account_label_exists)
            #print(quarter_id,name_account_item)
        #insert to quarter:
            quarter_id = cur.execute("INSERT INTO quarters (year, quarter_number, company_id) VALUES (%s, %s, %s) RETURNING id",
                            (year, quarter, company_id))
            cur.fetchone()  # Consume the result of the INSERT statement
            cur.execute("SELECT lastval()")
            quarter_id = cur.fetchone()[0]
            #print(quarter_id)
            #print('step 8')
        # Insert a new financial statement record for the balance sheet as of the end of the quarter (you can adjust the date as needed)
            financial_statement_id = cur.execute("INSERT INTO financial_statements (type, date, currency, quarter_id) VALUES (%s, %s, %s, %s) RETURNING id",
                                (fs_type, date_fs, currency, quarter_id))
            cur.fetchone()  # Consume the result of the INSERT statement
            cur.execute("SELECT lastval()")
            financial_statement_id = cur.fetchone()[0]
            #print(financial_statement_id)
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


#add_new_statement('NVS')
#exit()




def extract_revenue_last_2_years():


    query= '''
    SELECT * from companies
    WHERE ticker_symbol = 'AXP'
    '''

        # Execute the query and fetch the results

    df = pd.read_sql_query(query, conn)
    print(df)

#extract_revenue_last_2_years()
#exit()
#extract_revenue_last_2_years()

#conn = sqlite3.connect('financial_statements_SEC_EDGAR.db')
headers = {'User-Agent': 'codingandfun@gmail.com'}
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
        #time.sleep(1)
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
        #company_info_url = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'
        #company_info_response = requests.get(company_info_url,headers=headers)
        #company_info = json.loads(company_info_response.text)
        add_new_income_statement()
        #print(value,entity_name,cik,ticker, date_fs)
        #print(company_info)
        #if count >3:
        #    print(value)
        #   exit()
    #print(tag_counter)
#print(records_added, records_added_2,records_added_1)
conn.close()
logging.shutdown()
