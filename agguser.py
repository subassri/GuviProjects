import pymysql
import json
import os
import pandas as pd

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
CREATE TABLE IF NOT EXISTS aggregate_user (
    State VARCHAR(100),
    Year INT,
    Quarter INT,
    registeredUsers BIGINT,
    appOpens BIGINT
)
"""
cursor.execute(create_query)
conn.commit()

# Insert data
path = r'C:\Users\Administrator\Desktop\Miniproj\venv\pulse\data\aggregated\user\country\india'

data_list = []
for root, dirs, files in os.walk(path):
    for file in files:
        if file.endswith('.json'):
            file_path = os.path.join(root, file)
            with open(file_path, 'r') as f:
                data = json.load(f)
                state = file_path.split('\\')[-3].replace('%26', '&').replace('-', ' ').title()
                year = int(file_path.split('\\')[-2])
                quarter = int(file_path.split('\\')[-1].strip('.json'))
                data_dict = {
                    "State": state,
                    "Year": year,
                    "Quarter": quarter,
                    "registeredUsers": data['data']['aggregated']['registeredUsers'],
                    "appOpens": data['data']['aggregated']['appOpens']
                }
                data_list.append(data_dict)

df = pd.DataFrame(data_list)
for index, row in df.iterrows():
    insert_query = """
    INSERT INTO aggregate_user (State, Year, Quarter, registeredUsers, appOpens)
    VALUES (%s, %s, %s, %s, %s)
    """
    values = (row['State'], row['Year'], row['Quarter'], row['registeredUsers'], row['appOpens'])
    cursor.execute(insert_query, values)
conn.commit()

# Count rows
count_query = "SELECT COUNT(*) FROM aggregate_user"
cursor.execute(count_query)
count = cursor.fetchone()
print(f"Number of rows in aggregate_user table: {count[0]}")
conn.close()
