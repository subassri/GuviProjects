import pymysql
import json
import os

# DB credentials
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = 'test123456'
DB_NAME = 'phonepulseph'

# Create table
conn = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)
cursor = conn.cursor()

create_query = """ 
CREATE TABLE IF NOT EXISTS new_agginsu (
    State VARCHAR(100),
    Year INT,
    Quarter INT,
    Premium BIGINT,
    Count BIGINT
)
"""
cursor.execute(create_query)
conn.commit()

# Insert data
path = r'C:\Users\Administrator\Desktop\Miniproj\venv\pulse\data\aggregated\insurance\country\india\state'
for state_folder in os.listdir(path):
    state_path = os.path.join(path, state_folder)
    for year_folder in os.listdir(state_path):
        year_path = os.path.join(state_path, year_folder)
        for file in os.listdir(year_path):
            file_path = os.path.join(year_path, file)
            quarter = int(file.split('.')[0])
            with open(file_path, 'r') as f:
                data = json.load(f)
                try:
                    if 'transactionData' in data['data'] and len(data['data']['transactionData']) > 0:
                        if 'paymentInstruments' in data['data']['transactionData'][0] and len(data['data']['transactionData'][0]['paymentInstruments']) > 0:
                            premium = data['data']['transactionData'][0]['paymentInstruments'][0]['amount']
                            count = data['data']['transactionData'][0]['paymentInstruments'][0]['count']
                            insert_query = """ 
                            INSERT INTO new_agginsu (State, Year, Quarter, Premium, Count)
                            VALUES (%s, %s, %s, %s, %s)
                            """
                            values = (state_folder, int(year_folder), quarter, premium, count)
                            cursor.execute(insert_query, values)
                            conn.commit()
                except Exception as e:
                    print(f"Error inserting row: {e}")

# Count rows
count_query = "SELECT COUNT(*) FROM new_agginsu"
cursor.execute(count_query)
count = cursor.fetchone()
print(f"Number of rows in new_agginsu table: {count[0]}")

conn.close()

