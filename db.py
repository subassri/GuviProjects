import streamlit as st
import pandas as pd
import plotly.express as px
import pymysql

class PhonePeAnalysis:
    def __init__(self):
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='test123456',
            database='phonepulseph'
        )
        self.queries = {
            'Decoding Transaction Dynamics': {
                'query': """ 
                SELECT State, Year, Quater, Transaction_type, SUM(Transaction_count) as Transaction_count, SUM(Transaction_amount) as Transaction_amount 
                FROM aggregate_transaction 
                GROUP BY State, Year, Quater, Transaction_type 
                """
            },
            'Device Dominance': {
                'query': """ 
                SELECT State, Year, Quarter, SUM(registeredUsers) as registered_users, SUM(appOpens) as app_opens 
                FROM aggsusernew 
                GROUP BY State, Year, Quarter 
                """
            },
            'Insurance Penetration': {
                'query': """ 
                SELECT State, Year, Quarter, SUM(Premium) as premium, SUM(Count) as count 
                FROM new_agginsu 
                GROUP BY State, Year, Quarter 
                """
            }
        }
        self.dfs = {}
        self.load_data()

    def load_data(self):
        for category, query_dict in self.queries.items():
            try:
                df = pd.read_sql_query(query_dict['query'], self.conn)
                print(f"Loaded data for {category}:")
                print(df.head())
                self.dfs[category] = df
            except Exception as e:
                print(f"Error loading data: {e}")
        self.conn.close()

    def show_dashboard(self, category):
        categories = list(self.queries.keys())
        index = categories.index(category)
        df_query = list(self.dfs.values())[index]
        if df_query.empty:
            st.write("No data available")
            print("No data available for this category")
        else:
            st.write(df_query)
            if index == 0:
                fig = px.bar(df_query, x="State", y="Transaction_count", color="Transaction_type")
                st.plotly_chart(fig)
            elif index == 1:
                fig = px.bar(df_query, x="State", y="registered_users")
                st.plotly_chart(fig)
            elif index == 2:
                fig = px.bar(df_query, x="State", y="premium")
                st.plotly_chart(fig)

def main():
    st.title('PhonePe Analysis')
    analysis = PhonePeAnalysis()
    menu = st.sidebar.selectbox('Menu', ['Home', 'Dashboard'])
    if menu == 'Home':
        st.write('Welcome to PhonePe Analysis!')
    elif menu == 'Dashboard':
        category = st.selectbox('Select Category', list(analysis.queries.keys()))
        analysis.show_dashboard(category)

if __name__ == '__main__':
    main()