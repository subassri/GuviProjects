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
CREATE TABLE IF NOT EXISTS top_insurance (
    InsuranceCategory VARCHAR(100),
    Count BIGINT,
    Amount DECIMAL(20, 2)
)
"""
cursor.execute(create_query)
conn.commit()

# Insert data
path = r'C:\Users\Administrator\Desktop\Miniproj\venv\pulse\data\top\insurance\country\india'
data_list = []
for root, dirs, files in os.walk(path):
    for file in files:
        if file.endswith('.json'):
            file_path = os.path.join(root, file)
            with open(file_path, 'r') as f:
                data = json.load(f)
                if 'data' in data and data['data'] is not None:
                    if 'districts' in data['data'] and data['data']['districts'] is not None:
                        for district in data['data']['districts']:
                            data_dict = {
                                "InsuranceCategory": district['entityName'],
                                "Count": district['metric']['count'],
                                "Amount": district['metric']['amount']
                            }
                            data_list.append(data_dict)
                    if 'pincodes' in data['data'] and data['data']['pincodes'] is not None:
                        for pincode in data['data']['pincodes']:
                            data_dict = {
                                "InsuranceCategory": pincode['entityName'],
                                "Count": pincode['metric']['count'],
                                "Amount": pincode['metric']['amount']
                            }
                            data_list.append(data_dict)

df = pd.DataFrame(data_list)
for index, row in df.iterrows():
    insert_query = """ 
    INSERT INTO top_insurance (InsuranceCategory, Count, Amount)
    VALUES (%s, %s, %s)
    """
    values = (row['InsuranceCategory'], row['Count'], row['Amount'])
    cursor.execute(insert_query, values)
conn.commit()

# Count rows
count_query = "SELECT COUNT(*) FROM top_insurance"
cursor.execute(count_query)
count = cursor.fetchone()
print(f"Number of rows in top_insurance table: {count[0]}")
conn.close()
