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
            },
            'Map Transaction Analysis': {
                'query': """ 
                SELECT State, District, SUM(Count) as count, SUM(Amount) as amount 
                FROM map_map 
                GROUP BY State, District 
                """
            },
            'User Engagement and Growth Strategy': {
                'query': """ 
                SELECT State, District, SUM(Registered_Users) as registered_users 
                FROM topusernew 
                GROUP BY State, District 
                """
            }
        }
        self.dfs = {}
        self.load_data()
        self.transaction_count_by_state = pd.read_sql_query("SELECT State, SUM(Transaction_count) as transaction_count FROM aggregate_transaction GROUP BY State", self.conn)
        self.conn.close()

    def load_data(self):
        for category, query_dict in self.queries.items():
            try:
                df = pd.read_sql_query(query_dict['query'], self.conn)
                print(f"Loaded data for {category}:")
                print(df.head())
                self.dfs[category] = df
            except Exception as e:
                print(f"Error loading data: {e}")

    def show_home_page(self):
        st.write('Welcome to PhonePe Analysis!')
        fig = px.choropleth(
            self.transaction_count_by_state,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            color='transaction_count',
            color_continuous_scale='Reds'
        )
        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig)

    def show_dashboard(self, category):
        df_query = self.dfs.get(category)
        if df_query is None or df_query.empty:
            st.write("No data available")
            print("No data available for this category")
        else:
            st.write(df_query)
            if category == 'Decoding Transaction Dynamics':
                fig1 = px.bar(df_query, x="State", y="Transaction_count", color="Transaction_type", barmode='group')
                fig2 = px.scatter(df_query, x="State", y="Transaction_amount", color="Transaction_type")
                fig3 = px.line(df_query.groupby('State')['Transaction_count'].sum().reset_index(), x="State", y="Transaction_count")
                fig4 = px.pie(df_query, values="Transaction_count", names="Transaction_type")
                fig5 = px.histogram(df_query, x="Transaction_count", color="Transaction_type")
                st.plotly_chart(fig1)
                st.plotly_chart(fig2)
                st.plotly_chart(fig3)
                st.plotly_chart(fig4)
                st.plotly_chart(fig5)
            elif category == 'Device Dominance':
                fig1 = px.bar(df_query.groupby('State')['registered_users'].sum().reset_index(), x="State", y="registered_users")
                fig2 = px.scatter(df_query, x="State", y="app_opens")
                fig3 = px.line(df_query.groupby('State')['registered_users'].sum().reset_index(), x="State", y="registered_users")
                fig4 = px.pie(df_query.groupby('State')['registered_users'].sum().reset_index(), values="registered_users", names="State")
                fig5 = px.histogram(df_query, x="registered_users")
                st.plotly_chart(fig1)
                st.plotly_chart(fig2)
                st.plotly_chart(fig3)
                st.plotly_chart(fig4)
                st.plotly_chart(fig5)
            elif category == 'Insurance Penetration':
               fig1 = px.bar(df_query.groupby('State')['premium'].sum().reset_index(), x="State", y="premium")
               fig2 = px.scatter(df_query.groupby('State')['count'].sum().reset_index(), x="State", y="count")
               fig3 = px.line(df_query.groupby('State')['premium'].sum().reset_index(), x="State", y="premium")
               fig4 = px.pie(df_query.groupby('State')['premium'].sum().reset_index(), values="premium", names="State")
               fig5 = px.histogram(df_query, x="premium")
               st.plotly_chart(fig1)
               st.plotly_chart(fig2)
               st.plotly_chart(fig3)
               st.plotly_chart(fig4)
               st.plotly_chart(fig5)
            elif category == 'Map Transaction Analysis':
              fig1 = px.bar(df_query.groupby('State')['amount'].sum().reset_index(), x="State", y="amount")
              fig2 = px.scatter(df_query.groupby('State')['count'].sum().reset_index(), x="State", y="count")
              fig3 = px.line(df_query.groupby('State')['amount'].sum().reset_index(), x="State", y="amount")
              fig4 = px.pie(df_query.groupby('State')['amount'].sum().reset_index(), values="amount", names="State")
              fig5 = px.histogram(df_query, x="amount")
              st.plotly_chart(fig1)
              st.plotly_chart(fig2)
              st.plotly_chart(fig3)
              st.plotly_chart(fig4)
              st.plotly_chart(fig5)
            elif category == 'User Engagement and Growth Strategy':
             fig1 = px.bar(df_query.groupby('State')['registered_users'].sum().reset_index(), x="State", y="registered_users")
             fig2 = px.scatter(df_query, x="District", y="registered_users")
             fig3 = px.line(df_query.groupby('State')['registered_users'].sum().reset_index(), x="State", y="registered_users")
             fig4 = px.pie(df_query.groupby('State')['registered_users'].sum().reset_index(), values="registered_users", names="State")
             fig5 = px.histogram(df_query, x="registered_users")
             st.plotly_chart(fig1)
             st.plotly_chart(fig2)
             st.plotly_chart(fig3)
             st.plotly_chart(fig4)
             st.plotly_chart(fig5)
def main():
    st.title('PhonePe Analysis')
    analysis = PhonePeAnalysis()
    menu = st.sidebar.selectbox('Menu', ['Home', 'Dashboard'])
    if menu == 'Home':
        analysis.show_home_page()
    elif menu == 'Dashboard':
        category = st.selectbox('Select Category', list(analysis.queries.keys()))
        analysis.show_dashboard(category)

if __name__ == '__main__':
    main()




