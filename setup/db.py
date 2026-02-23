import psycopg2
from psycopg2 import sql 

DB_CONFIG = {
    "dbname": "benchmark",
    "user": "siddhant",
    "password": "SIDDHANT13@@rn@v25",
    "host": "localhost",
    "port": 5432
}

def get_db_connection():
    try:
        connection = psycopg2.connect(**DB_CONFIG) #what is the understandiung behind ** and where is it used?
        return connection
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        exit(1)
