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
CREATE TABLE IF NOT EXISTS topinsurancenew (
    State VARCHAR(100),
    District VARCHAR(100),
    Pincode VARCHAR(100),
    Year INT,
    Quarter INT,
    Count BIGINT,
    Amount DECIMAL(20, 2)
)
"""
cursor.execute(create_query)
conn.commit()

# Insert data
path = r'C:\Users\Administrator\Desktop\Miniproj\venv\pulse\data\top\insurance\country\india\state'
for state in os.listdir(path):
    print(f"State: {state}")  # Print the state name
    state_path = os.path.join(path, state)
    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)
        for file in os.listdir(year_path):
            if file.endswith('.json'):
                file_path = os.path.join(year_path, file)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    if 'data' in data and data['data'] is not None:
                        if 'districts' in data['data'] and data['data']['districts'] is not None:
                            for district in data['data']['districts']:
                                insert_query = """ 
                                INSERT INTO topinsurancenew (State, District, Pincode, Year, Quarter, Count, Amount)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """
                                values = (state, district['entityName'], '', int(year), int(file.strip('.json')), district['metric']['count'], district['metric']['amount'])
                                cursor.execute(insert_query, values)
                                conn.commit()
                        if 'pincodes' in data['data'] and data['data']['pincodes'] is not None:
                            for pincode in data['data']['pincodes']:
                                insert_query = """ 
                                INSERT INTO topinsurancenew (State, District, Pincode, Year, Quarter, Count, Amount)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """
                                values = (state, '', pincode['entityName'], int(year), int(file.strip('.json')), pincode['metric']['count'], pincode['metric']['amount'])
                                cursor.execute(insert_query, values)
                                conn.commit()

# Count rows
count_query = "SELECT COUNT(*) FROM topinsurancenew"
cursor.execute(count_query)
count = cursor.fetchone()
print(f"Number of rows in topinsurancenew table: {count[0]}")

conn.close()

