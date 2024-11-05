import psycopg2
import pandas as pd

# Load the CSV file
csv_file = 'code_stock.csv'
df = pd.read_csv(csv_file)

# PostgreSQL connection information
db_params = {
    'dbname': 'stock',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'localhost',
    'port': '5432'
}

# Connect to PostgreSQL
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Create the table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock (
        code VARCHAR(1000) PRIMARY KEY,  -- MÃ CK (Ticker), set as the primary key
        name TEXT,               -- TÊN CÔNG TY (Company Name)
        market VARCHAR(1000)                 -- SÀN (Stock Exchange)
    )
''')

# Insert data into the table
for index, row in df.iterrows():
    ma_ck = row['STOCK_CODE']
    ten_cong_ty = row['COMPANY_NAME']
    san = row['MARKET']  # Ensure the column name matches exactly
    
    cursor.execute('''
        INSERT INTO stock (code, name, market) 
        VALUES (%s, %s, %s)
        ON CONFLICT (code) DO NOTHING
    ''', (ma_ck, ten_cong_ty, san))

# Commit the transaction
conn.commit()

# Close the connection
cursor.close()
conn.close()

print("Data inserted successfully!")
