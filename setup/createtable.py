from psycopg2 import sql 

def create_table(cursor , table_name):
    query = sql.SQL("""
                    DROP TABLE IF EXISTS {table};
                    CREATE TABLE {table} (
                        show_id TEXT PRIMARY KEY,
                        type TEXT,
                        title TEXT,
                        director TEXT,
                        "cast" TEXT[],
                        country TEXT,
                        date_added TEXT,
                        release_year INT,
                        rating TEXT,
                        duration TEXT,
                        listed_in TEXT,
                        description TEXT);
                    )
                    """).format(table=sql.Identifier(table_name))
    
    cursor.execute(query);
    
    
    


#need to store cast in a better way 