import streamlit as st
import pandas as pd
import pymysql
import matplotlib.pyplot as plt
import seaborn as sns

# Define the database connection parameters
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = 'test123456'
DB_NAME = 'phonepe_pulse_v2'

# Function to fetch data from the database
def fetch_data(table_name):
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table_name}")
        data = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        conn.close()
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        print(f"An error occurred: {e}")

# Fetch data from the database
df = fetch_data('Aggregated_user')

# Case Study 1: Decoding Transaction Dynamics on PhonePe
def transaction_dynamics(df):
    # Create bar chart
    fig = plt.figure(figsize=(10, 6))
    sns.barplot(x='user_id', y='aggregated_value', data=df)
    plt.title('Transaction Dynamics')
    plt.xlabel('User ID')
    plt.ylabel('Aggregated Value')
    st.pyplot(fig)

# Streamlit Dashboard
st.title('PhonePe Analysis Dashboard')

if df is not None:
    st.subheader('Aggregated User Data')
    st.write(df)

    st.subheader('Transaction Dynamics')
    transaction_dynamics(df)
else:
    st.write("No data available")
