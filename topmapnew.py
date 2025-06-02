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
CREATE TABLE IF NOT EXISTS Top_map (
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
path = r'C:\Users\Administrator\Desktop\Miniproj\venv\pulse\data\top\transaction\country\india\state'

data_dict = {
    'State': [],
    'District': [],
    'Pincode': [],
    'Year': [],
    'Quarter': [],
    'Count': [],
    'Amount': []
}

for state in os.listdir(path):
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
                                data_dict['State'].append(state)
                                data_dict['District'].append(district['entityName'])
                                data_dict['Pincode'].append('')
                                data_dict['Year'].append(int(year))
                                data_dict['Quarter'].append(int(file.strip('.json')))
                                data_dict['Count'].append(district['metric']['count'])
                                data_dict['Amount'].append(district['metric']['amount'])
                        if 'pincodes' in data['data'] and data['data']['pincodes'] is not None:
                            for pincode in data['data']['pincodes']:
                                data_dict['State'].append(state)
                                data_dict['District'].append('')
                                data_dict['Pincode'].append(pincode['entityName'])
                                data_dict['Year'].append(int(year))
                                data_dict['Quarter'].append(int(file.strip('.json')))
                                data_dict['Count'].append(pincode['metric']['count'])
                                data_dict['Amount'].append(pincode['metric']['amount'])

for i in range(len(data_dict['State'])):
    insert_query = """ 
    INSERT INTO Top_map (State, District, Pincode, Year, Quarter, Count, Amount)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    values = (data_dict['State'][i], data_dict['District'][i], data_dict['Pincode'][i], data_dict['Year'][i], data_dict['Quarter'][i], data_dict['Count'][i], data_dict['Amount'][i])
    cursor.execute(insert_query, values)
    conn.commit()

# Count rows
count_query = "SELECT COUNT(*) FROM Top_map"
cursor.execute(count_query)
count = cursor.fetchone()
print(f"Number of rows in Top_map table: {count[0]}")

conn.close()
