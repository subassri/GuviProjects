import pymysql
import pandas as pd
import os
import json


# Database credentials
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = 'test123456'
DB_NAME = 'phonepe_pulse_new'

try:
    # Establish a connection to the MySQL database
    db = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

    print("Connected to MySQL server")

    # Create a cursor object
    cursor = db.cursor()

    # Define the path
    path = "data/aggregated/transaction/country/india/state/"

    # Get the list of states
    Agg_state_list = os.listdir(path)

    clm = {'State': [], 'Year': [], 'Quater': [], 'Transacion_type': [], 'Transacion_count': [], 'Transacion_amount': []}

    for i in Agg_state_list:
        p_i = path + i + "/"
        Agg_yr = os.listdir(p_i)
        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)
            for k in Agg_yr_list:
                p_k = p_j + k
                Data = open(p_k, 'r')
                D = json.load(Data)
                for z in D['data']['transactionData']:
                    Name = z['name']
                    count = z['paymentInstruments'][0]['count']
                    amount = z['paymentInstruments'][0]['amount']
                    clm['Transacion_type'].append(Name)
                    clm['Transacion_count'].append(count)
                    clm['Transacion_amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))

    Agg_Trans = pd.DataFrame(clm)

    # Insert data into the Aggregated_transaction table
    print("Inserting data into Aggregated_transaction table...")
    for index, row in Agg_Trans.iterrows():
        try:
            cursor.execute("""
                INSERT INTO Aggregated_transaction (state, year, quarter, transaction_type, transaction_count, transaction_amount)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (row['State'], row['Year'], row['Quater'], row['Transacion_type'], row['Transacion_count'], row['Transacion_amount']))
        except Exception as e:
            print(f"Error inserting row {index}: {e}")
    print("Data inserted successfully")

    # Commit the changes
    db.commit()
    print("Changes committed")

    # Close the cursor and connection
    cursor.close()
    db.close()
except pymysql.Error as err:
    print("Something went wrong: {}".format(err))
