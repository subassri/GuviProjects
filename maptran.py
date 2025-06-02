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
CREATE TABLE IF NOT EXISTS map_map (
    State VARCHAR(100),
    District VARCHAR(100),
    Count BIGINT,
    Amount DECIMAL(20, 2)
)
"""
cursor.execute(create_query)
conn.commit()

# Insert data
path = r'C:\Users\Administrator\Desktop\Miniproj\venv\pulse\data\map\transaction\hover\country\india'
data_list = []
for root, dirs, files in os.walk(path):
    for file in files:
        if file.endswith('.json'):
            file_path = os.path.join(root, file)
            state = None
            path_parts = file_path.split('\\')
            for i, part in enumerate(path_parts):
                if part == 'state' and i + 1 < len(path_parts):
                    state = path_parts[i + 1].replace('-', ' ').title()
            if state:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    if 'hoverDataList' in data['data'] and data['data']['hoverDataList'] is not None:
                        for hoverData in data['data']['hoverDataList']:
                            if 'metric' in hoverData and hoverData['metric']:
                                metric = hoverData['metric'][0]
                                data_dict = {
                                    "State": state,
                                    "District": hoverData.get('name', ''),
                                    "Count": metric.get('count', 0),
                                    "Amount": metric.get('amount', 0.0)
                                }
                                print(f"State: {data_dict['State']}, District: {data_dict['District']}, Count: {data_dict['Count']}, Amount: {data_dict['Amount']}")
                                data_list.append(data_dict)

df = pd.DataFrame(data_list)
print(df.head())
for index, row in df.iterrows():
    insert_query = """ 
    INSERT INTO map_map (State, District, Count, Amount)
    VALUES (%s, %s, %s, %s)
    """
    values = (row['State'], row['District'], row['Count'], row['Amount'])
    cursor.execute(insert_query, values)
    conn.commit()

# Count rows
count_query = "SELECT COUNT(*) FROM map_map"
cursor.execute(count_query)
count = cursor.fetchone()
print(f"Number of rows in map_map table: {count[0]}")

conn.close()

