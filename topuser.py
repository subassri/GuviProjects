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
CREATE TABLE IF NOT EXISTS top_user (
    UserType VARCHAR(100),
    RegisteredUsers BIGINT,
    AppOpens BIGINT
)
"""
cursor.execute(create_query)
conn.commit()

# Insert data
path = r'C:\Users\Administrator\Desktop\Miniproj\venv\pulse\data\top\user\country\india'
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
                            "UserType": district['name'],
                            "RegisteredUsers": district.get('registeredUsers', 0),
                            "AppOpens": district.get('appOpens', 0)
                        }
                        print(f"District: {data_dict['UserType']}, Registered Users: {data_dict['RegisteredUsers']}, App Opens: {data_dict['AppOpens']}")
                        data_list.append(data_dict)
                if 'states' in data['data'] and data['data']['states'] is not None:
                    for state in data['data']['states']:
                        data_dict = {
                            "UserType": state['name'],
                            "RegisteredUsers": state.get('registeredUsers', 0),
                            "AppOpens": state.get('appOpens', 0)
                        }
                        print(f"State: {data_dict['UserType']}, Registered Users: {data_dict['RegisteredUsers']}, App Opens: {data_dict['AppOpens']}")
                        data_list.append(data_dict)

df = pd.DataFrame(data_list)
print(df.head())

for index, row in df.iterrows():
    insert_query = """ 
    INSERT INTO top_user (UserType, RegisteredUsers, AppOpens)
    VALUES (%s, %s, %s)
    """
    values = (row['UserType'], row['RegisteredUsers'], row['AppOpens'])
    cursor.execute(insert_query, values)
conn.commit()

# Count rows
count_query = "SELECT COUNT(*) FROM top_user"
cursor.execute(count_query)
count = cursor.fetchone()
print(f"Number of rows in top_user table: {count[0]}")

conn.close()
