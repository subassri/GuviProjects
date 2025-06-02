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
CREATE TABLE IF NOT EXISTS aggsusernew (
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
path = r'C:\Users\Administrator\Desktop\Miniproj\venv\pulse\data\aggregated\user\country\india\state'
data_dict = {
    'State': [],
    'Year': [],
    'Quarter': [],
    'registeredUsers': [],
    'appOpens': []
}
for state in os.listdir(path):
    state_path = os.path.join(path, state)
    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)
        for file in os.listdir(year_path):
            file_path = os.path.join(year_path, file)
            quarter = int(file.strip('.json'))
            with open(file_path, 'r') as f:
                data = json.load(f)
                data_dict['State'].append(state)
                data_dict['Year'].append(int(year))
                data_dict['Quarter'].append(quarter)
                data_dict['registeredUsers'].append(data['data']['aggregated']['registeredUsers'])
                data_dict['appOpens'].append(data['data']['aggregated']['appOpens'])

for i in range(len(data_dict['State'])):
    insert_query = """ 
    INSERT INTO aggsusernew (State, Year, Quarter, registeredUsers, appOpens)
    VALUES (%s, %s, %s, %s, %s)
    """
    values = (data_dict['State'][i], data_dict['Year'][i], data_dict['Quarter'][i], data_dict['registeredUsers'][i], data_dict['appOpens'][i])
    cursor.execute(insert_query, values)
    conn.commit()

# Count rows
count_query = "SELECT COUNT(*) FROM aggsusernew"
cursor.execute(count_query)
count = cursor.fetchone()
print(f"Number of rows in aggsusernew table: {count[0]}")

conn.close()
