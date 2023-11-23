import psycopg2

# Connect to your PostgreSQL database
conn = psycopg2.connect("dbname=your_db user=your_user password=your_password")
cur = conn.cursor()

# Fetch required data
cur.execute("""
    SELECT 
        q.company_id, q.year, q.quarter_number, 
        fs.id AS financial_statement_id,
        fsi1.value AS current_assets, 
        fsi2.value AS current_liabilities
    FROM 
        financial_statements fs
    JOIN 
        financial_statement_items fsi1 ON fs.id = fsi1.financial_statement_id AND fsi1.account_label = 'Current Assets'
    JOIN 
        financial_statement_items fsi2 ON fs.id = fsi2.financial_statement_id AND fsi2.account_label = 'Current Liabilities'
    JOIN 
        quarters q ON fs.quarter_id = q.id
""")
rows = cur.fetchall()

# Calculate and insert ratios
for row in rows:
    company_id, year, quarter_number, financial_statement_id, current_assets, current_liabilities = row
    current_ratio = current_assets / current_liabilities if current_liabilities != 0 else None

    if current_ratio is not None:
        cur.execute("""
            INSERT INTO financial_ratios (quarter_id, ratio_name, ratio_value)
            VALUES (%s, 'Current Ratio', %s)
        """, (financial_statement_id, current_ratio))

# Commit changes and close connection
conn.commit()
cur.close()
conn.close()