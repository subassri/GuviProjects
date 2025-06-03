import streamlit as st
import pandas as pd
import plotly.express as px
import pymysql
import warnings
warnings.filterwarnings('ignore')

class PhonePeAnalysis:
    def __init__(self):
        try:
            self.conn = pymysql.connect(
                host='localhost',
                user='root',
                password='test123456',
                database='phonepulseph'
            )
            print("Database connection successful")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            st.write(f"Error connecting to database: {e}")

        self.queries = {
            'Decoding Transaction Dynamics': {
                'bar_chart': """SELECT Transaction_type, SUM(Transaction_count) as Transaction_count FROM aggregate_transaction GROUP BY Transaction_type""",
                'scatter_plot': """SELECT Year, AVG(Transaction_amount) as avg_amount FROM aggregate_transaction GROUP BY Year""",
                'line_chart': """SELECT Transaction_type, MAX(Transaction_count) as max_count FROM aggregate_transaction GROUP BY Transaction_type""",
                'pie_chart': """SELECT Transaction_type, COUNT(*) as count FROM aggregate_transaction GROUP BY Transaction_type""",
                'histogram': """SELECT Transaction_count FROM aggregate_transaction"""
            },
            'Device Dominance': {
                'bar_chart': """SELECT State, SUM(registeredUsers) as registered_users FROM aggsusernew GROUP BY State""",
                'scatter_plot': """SELECT Year, AVG(registeredUsers) as avg_registered FROM aggsusernew GROUP BY Year""",
                'line_chart': """SELECT State, MAX(registeredUsers) as max_registered FROM aggsusernew GROUP BY State""",
                'pie_chart': """SELECT State, COUNT(*) as count FROM aggsusernew GROUP BY State""",
                'histogram': """SELECT registeredUsers FROM aggsusernew"""
            },
            'Insurance Penetration': {
                'bar_chart': """SELECT State, SUM(Premium) as premium FROM new_agginsu GROUP BY State""",
                'scatter_plot': """SELECT Year, AVG(Premium) as avg_premium FROM new_agginsu GROUP BY Year""",
                'line_chart': """SELECT State, MAX(Premium) as max_premium FROM new_agginsu GROUP BY State""",
                'pie_chart': """SELECT State, COUNT(*) as count FROM new_agginsu GROUP BY State""",
                'histogram': """SELECT Premium FROM new_agginsu"""
            },
            'Map Transaction Analysis': {
                'bar_chart': """SELECT State, SUM(Amount) as amount FROM map_map GROUP BY State""",
                'pie_chart': """SELECT State, COUNT(*) as count FROM map_map GROUP BY State""",
                'histogram': """SELECT Amount FROM map_map"""
            },
            'User Engagement and Growth Strategy': {
                'bar_chart': """SELECT State, SUM(Registered_Users) as registered_users FROM topusernew GROUP BY State""",
                'scatter_plot': """SELECT Year, AVG(Registered_Users) as avg_registered FROM topusernew GROUP BY Year""",
                'line_chart': """SELECT State, MAX(Registered_Users) as max_registered FROM topusernew GROUP BY State""",
                'pie_chart': """SELECT State, COUNT(*) as count FROM topusernew GROUP BY State""",
                'histogram': """SELECT Registered_Users FROM topusernew"""
            }
        }

        self.dfs = {}
        self.load_data()
        self.transaction_count_by_state = pd.read_sql_query("SELECT State, SUM(Transaction_count) as transaction_count FROM aggregate_transaction GROUP BY State", self.conn)

    def close_connection(self):
        try:
            self.conn.close()
            print("Database connection closed")
        except Exception as e:
            print(f"Error closing database connection: {e}")

    def load_data(self):
        for category, queries in self.queries.items():
            self.dfs[category] = {}
            for chart_type, query in queries.items():
                try:
                    df = pd.read_sql_query(query, self.conn)
                    print(f"Loaded data for {category} - {chart_type}:")
                    print(df.head())
                    self.dfs[category][chart_type] = df
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
       st.plotly_chart(fig, use_container_width=True)

    def show_dashboard(self, category):
      try:
        print(f"Showing dashboard for category: {category}")
        if category not in self.dfs:
            st.write("No data available")
            print(f"No data available for category: {category}")
            return
        df_query = self.dfs[category]
        print(f"df_query type: {type(df_query)}")
        print(f"df_query keys: {df_query.keys()}")
        print(f"df_query values: {df_query.values()}")
        st.subheader(category)
        for chart_type, df in df_query.items():
            try:
                print(f"Displaying {chart_type} for category: {category}")
                print(f"df type: {type(df)}")
                print(f"df columns: {df.columns}")
                st.write(f"{chart_type.capitalize().replace('_', ' ')}")
                st.dataframe(df)
                if chart_type == 'bar_chart':
                    if len(df.columns) > 2:
                        fig = px.bar(df, x=df.columns[0], y=df.columns[1], color=df.columns[2])
                    else:
                        fig = px.bar(df, x=df.columns[0], y=df.columns[1])
                    st.plotly_chart(fig, use_container_width=True)
                elif chart_type == 'scatter_plot':
                    if len(df.columns) > 2:
                        fig = px.scatter(df, x=df.columns[0], y=df.columns[1], color=df.columns[2])
                    else:
                        fig = px.scatter(df, x=df.columns[0], y=df.columns[1])
                    st.plotly_chart(fig, use_container_width=True)
                elif chart_type == 'line_chart':
                    if len(df.columns) > 2:
                        fig = px.line(df, x=df.columns[0], y=df.columns[1], color=df.columns[2])
                    else:
                        fig = px.line(df, x=df.columns[0], y=df.columns[1])
                    st.plotly_chart(fig, use_container_width=True)
                elif chart_type == 'pie_chart':
                    fig = px.pie(df, values=df.columns[1], names=df.columns[0])
                    st.plotly_chart(fig, use_container_width=True)
                elif chart_type == 'histogram':
                    if len(df.columns) > 0 and df.dtypes[0].kind in 'bifc':
                        fig = px.histogram(df, x=df.columns[0])
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.write("Error: DataFrame is empty or column type is not numeric.")
            except Exception as e:
                st.write(f"Error displaying {chart_type}: {e}")
                print(f"Error displaying {chart_type}: {e}")
      except Exception as e:
        st.write(f"An error occurred: {e}")
        print(f"An error occurred: {e}")
def main():
    print("Main function started")
    st.title('PhonePe Analysis')
    analysis = PhonePeAnalysis()
    try:
        menu = st.sidebar.selectbox('Menu', ['Home', 'Dashboard'])
        print(f"Menu selected: {menu}")
        if menu == 'Home':
            print("Showing home page")
            analysis.show_home_page()
        elif menu == 'Dashboard':
            try:
                category = st.selectbox('Select Category', list(analysis.queries.keys()))
                print(f"Category selected: {category}")
                if category:
                    print("Showing dashboard")
                    analysis.show_dashboard(category)
            except Exception as e:
                st.write(f"An error occurred in dashboard: {e}")
                print(f"An error occurred in dashboard: {e}")
    except Exception as e:
        st.write(f"An error occurred: {e}")
        print(f"An error occurred: {e}")
    finally:
        analysis.close_connection()
        print("Database connection closed")

if __name__ == '__main__':
    main()

