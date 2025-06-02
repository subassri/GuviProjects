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
CREATE TABLE IF NOT EXISTS top_transaction (
    EntityType VARCHAR(100),
    EntityName VARCHAR(100),
    Count BIGINT,
    Amount DECIMAL(20, 2)
)
"""
cursor.execute(create_query)
conn.commit()

# Insert data
path = r'C:\Users\Administrator\Desktop\Miniproj\venv\pulse\data\top\transaction\country\india'
data_list = []
for root, dirs, files in os.walk(path):
    for file in files:
        if file.endswith('.json'):
            file_path = os.path.join(root, file)
            with open(file_path, 'r') as f:
                data = json.load(f)
                print(f"Processing file: {file_path}")
                if 'districts' in data['data'] and data['data']['districts'] is not None:
                    for district in data['data']['districts']:
                        data_dict = {
                            "EntityType": "District",
                            "EntityName": district['entityName'],
                            "Count": district['metric'].get('count', 0),
                            "Amount": district['metric'].get('amount', 0)
                        }
                        print(f"District: {data_dict['EntityName']}, Count: {data_dict['Count']}, Amount: {data_dict['Amount']}")
                        data_list.append(data_dict)
                if 'states' in data['data'] and data['data']['states'] is not None:
                    for state in data['data']['states']:
                        data_dict = {
                            "EntityType": "State",
                            "EntityName": state['entityName'],
                            "Count": state['metric'].get('count', 0),
                            "Amount": state['metric'].get('amount', 0)
                        }
                        print(f"State: {data_dict['EntityName']}, Count: {data_dict['Count']}, Amount: {data_dict['Amount']}")
                        data_list.append(data_dict)
                if 'pincodes' in data['data'] and data['data']['pincodes'] is not None:
                    for pincode in data['data']['pincodes']:
                        data_dict = {
                            "EntityType": "Pincode",
                            "EntityName": pincode['entityName'],
                            "Count": pincode['metric'].get('count', 0),
                            "Amount": pincode['metric'].get('amount', 0)
                        }
                        print(f"Pincode: {data_dict['EntityName']}, Count: {data_dict['Count']}, Amount: {data_dict['Amount']}")
                        data_list.append(data_dict)

df = pd.DataFrame(data_list)
print(df.head())
for index, row in df.iterrows():
    insert_query = """ 
    INSERT INTO top_transaction (EntityType, EntityName, Count, Amount)
    VALUES (%s, %s, %s, %s)
    """
    values = (row['EntityType'], row['EntityName'], row['Count'], row['Amount'])
    cursor.execute(insert_query, values)
conn.commit()

# Count rows
count_query = "SELECT COUNT(*) FROM top_transaction"
cursor.execute(count_query)
count = cursor.fetchone()
print(f"Number of rows in top_transaction table: {count[0]}")

conn.close()
