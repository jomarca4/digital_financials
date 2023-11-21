import psycopg2
from psycopg2 import OperationalError

# Database credentials
import os

db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_host = os.environ.get('DB_HOST')

def create_database_schema(cur):
    # Create companies table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            ticker_symbol TEXT NOT NULL UNIQUE,
            cik INTEGER NOT NULL UNIQUE,
            location TEXT NOT NULL
        )
    ''')

    # Create quarters table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS quarters (
            id SERIAL PRIMARY KEY,
            year INTEGER NOT NULL,
            quarter_number INTEGER NOT NULL,
            company_id INTEGER NOT NULL,
            FOREIGN KEY (company_id) REFERENCES companies (id)
        )
    ''')

    # Create financial statements table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS financial_statements (
            id SERIAL PRIMARY KEY,
            type TEXT NOT NULL,
            date DATE NOT NULL,
            currency TEXT NOT NULL,
            quarter_id INTEGER NOT NULL,
            FOREIGN KEY (quarter_id) REFERENCES quarters (id)
        )
    ''')

    # Create financial statement items table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS financial_statement_items (
            id SERIAL PRIMARY KEY,
            account_label TEXT NOT NULL,
            value DECIMAL NOT NULL,
            unit_of_measurement TEXT NOT NULL,
            financial_statement_id INTEGER NOT NULL,
            FOREIGN KEY (financial_statement_id) REFERENCES financial_statements (id)
        )
    ''')

    # Create financial ratios table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS financial_ratios (
            id SERIAL PRIMARY KEY,
            quarter_id INTEGER NOT NULL,
            ratio_name TEXT NOT NULL,
            ratio_value DECIMAL NOT NULL,
            FOREIGN KEY (quarter_id) REFERENCES quarters (id)
        )
    ''')

    # Create market data table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            date DATE NOT NULL,
            open_price DECIMAL,
            close_price DECIMAL,
            high_price DECIMAL,
            low_price DECIMAL,
            volume BIGINT,
            market_cap DECIMAL,
            FOREIGN KEY (company_id) REFERENCES companies (id)
        )
    ''')

    # Create company news and events table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS company_news (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            news_date DATE NOT NULL,
            headline TEXT,
            source TEXT,
            url TEXT,
            summary TEXT,
            FOREIGN KEY (company_id) REFERENCES companies (id)
        )
    ''')

    # Create dividends and stock splits table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS dividends_stock_splits (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            event_date DATE NOT NULL,
            event_type TEXT NOT NULL,
            value DECIMAL,
            FOREIGN KEY (company_id) REFERENCES companies (id)
        )
    ''')

    # Create shareholders table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS shareholders (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            shareholder_name TEXT NOT NULL,
            ownership_percentage DECIMAL,
            shares_held BIGINT,
            FOREIGN KEY (company_id) REFERENCES companies (id)
        )
    ''')

    # Create corporate governance table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS corporate_governance (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            member_name TEXT NOT NULL,
            position TEXT,
            biography TEXT,
            FOREIGN KEY (company_id) REFERENCES companies (id)
        )
    ''')

    # Create ESG scores table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS esg_scores (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            rating_date DATE NOT NULL,
            environmental_score DECIMAL,
            social_score DECIMAL,
            governance_score DECIMAL,
            overall_score DECIMAL,
            FOREIGN KEY (company_id) REFERENCES companies (id)
        )
    ''')

    # Create analyst estimates table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS analyst_estimates (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            analyst_name TEXT NOT NULL,
            estimate_date DATE NOT NULL,
            estimate_type TEXT NOT NULL,
            estimate_value DECIMAL NOT NULL,
            FOREIGN KEY (company_id) REFERENCES companies (id)
        )
    ''')
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
connection = create_connection(db_name, db_user, db_password, db_host)

# Create tables
if connection is not None:
    connection.autocommit = True
    cursor = connection.cursor()
    create_database_schema(cursor)
    cursor.close()
    connection.close()
else:
    print("Connection to PostgreSQL DB failed")