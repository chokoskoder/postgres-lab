from psycopg2.extras import execute_values
from psycopg2 import sql , Error

def insert_data(cursor , table_name , data):
    query = sql.SQL("INSERT INTO {} values %s").format(
        sql.Identifier(table_name)
    )
    try:
        execute_values(cursor , query ,data)
        print(f"Inserted into {table_name} {len(data)} rows")
        return len(data)
    except Error as db_error:
        raise RuntimeError(
            f"Database error while inserting into {table_name}"
         ) from db_error # this is exception chaining  , allows us to make sure that we see the error related to the msg above
        
    except Exception as e:
        raise RuntimeError(
            f"Unexpected error while loading the data"
        ) from e # this is exception chaining 