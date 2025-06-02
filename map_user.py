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
CREATE TABLE IF NOT EXISTS Mapuser (
    State VARCHAR(100),
    Year INT,
    District VARCHAR(100),
    RegisteredUsers BIGINT,
    AppOpens BIGINT
)
"""
cursor.execute(create_query)
conn.commit()

# Insert data
path = r'C:\Users\Administrator\Desktop\Miniproj\venv\pulse\data\map\user\hover\country\india'
data_list = []
for root, dirs, files in os.walk(path):
    for file in files:
        if file.endswith('.json'):
            file_path = os.path.join(root, file)
            state = None
            year = None
            path_parts = file_path.split('\\')
            for i, part in enumerate(path_parts):
                if part == 'state' and i + 1 < len(path_parts):
                    state = path_parts[i + 1].replace('-', ' ').title()
                if part.isdigit() and len(part) == 4:
                    year = int(part)
            if state:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                if 'hoverData' in data['data']:
                    for district, metrics in data['data']['hoverData'].items():
                        data_dict = {
                            "State": state,
                            "Year": year if year else 0,
                            "District": district.replace('-', ' ').title() if '-' in district else district.title(),
                            "RegisteredUsers": metrics.get('registeredUsers', 0),
                            "AppOpens": metrics.get('appOpens', 0)
                        }
                        print(f"State: {data_dict['State']}, Year: {data_dict['Year']}, District: {data_dict['District']}, RegisteredUsers: {data_dict['RegisteredUsers']}, AppOpens: {data_dict['AppOpens']}")
                        data_list.append(data_dict)

df = pd.DataFrame(data_list)
print(df.head())

for index, row in df.iterrows():
    insert_query = """ 
    INSERT INTO Mapuser (State, Year, District, RegisteredUsers, AppOpens)
    VALUES (%s, %s, %s, %s, %s)
    """
    values = (row['State'], row['Year'], row['District'], row['RegisteredUsers'], row['AppOpens'])
    cursor.execute(insert_query, values)
    conn.commit()

# Count rows
count_query = "SELECT COUNT(*) FROM Mapuser"
cursor.execute(count_query)
count = cursor.fetchone()
print(f"Number of rows in Mapuser table: {count[0]}")

conn.close()
