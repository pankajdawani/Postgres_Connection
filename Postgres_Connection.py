import psycopg2
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


class PostgresDB:
    def __init__(self, dbname, user, password, host="localhost", port="5432"):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            print("Connection to PostgreSQL DB successful")
        except Exception as e:
            print(f"Error: {e}")
    
    def execute_query(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
            print("Query executed successfully")
        except Exception as e:
            print(f"Error: {e}")
    
    def fetch_results_as_dataframe(self, query, params=None):
        try:
            df = pd.read_sql(query, self.connection, params=params)
            return df
        except Exception as e:
            print(f"Error: {e}")
            return None

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("PostgreSQL connection closed")


# we can test this code with insert and fetching the data in python
# 
# # Changes made again 
