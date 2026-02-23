from psycopg2 import sql
import time
from createtable import create_table
from load import insert_data

def setup_indexes(cursor, data_rows):
   """
   we will have 4 main different tables here:
   Pattern search -> LIKE/ILIKE
   Full-Text Search -> tsvector with/without GIN
   Vector search -> no index(exact neighbour) vs IVFFlat vs HNSW
   Array search -> no index vs GIN
   """
   
   #We will need these extensions to ensure smooth running of our tests:
   print("Enabling extensions")
   cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
   cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
   
   #we need to populate the master table which will be used as the source of truth and cloned for the tests
   
   print("creating the master table")
    # Creating the master table
   create_table(cursor , "netflix_master")
   insert_data(cursor , "netflix_master" , data_rows)
   
   print("\n setting up pattern search tables")
   
   #Basic table no indexes -> for standard LIKE
   cursor.execute("DROP TABLE IF EXISTS bench_pattern_heap;")
   cursor.execute("CREATE TABLE bench_pattern_heap AS TABLE netflix_master;")
   
   #Optimised table which enables ILIKE -> works best for pattern matching and PREFIX search
   cursor.execute("DROP TABLE IF EXISTS bench_pattern_index;")
   cursor.execute("CREATE TABLE bench_pattern_index AS TABLE netflix_master;")
   cursor.execute("CREATE INDEX idx_pattern_title ON bench_pattern_index (title text_pattern_ops);")
   
   #Optimised table which enable suffix and %any% pattern searching 
   cursor.execute("DROP TABLE IF EXISTS bench_pattern_trgm;")
   cursor.execute("CREATE TABLE bench_pattern_trgm AS TABLE netflix_master;")
   cursor.ecexute("CREATE INDEX idx_pattern_title ON bench_pattern_trgm USING GIN (title gin_trgm_ops)")
   
   
   print("\n Setting up table for Full-Text Search...")
   
   #we will create a sharred helper to make a tsvector column and populate it in differnt dbs with different indexes
   fts_prep_sql = """
   ALTER TABLE {table} ADD COLUMN search_vector tsvector;
   UPDATE {table}
   SET search_vector = to_tsvector('english' , coalesce(title , '') || ' ' || coalesce(description,''));
   """

   #tabl 1 no index , sequential scan 
   cursor.execute("DROP TABLE IF EXISTS bench_fts_heap;")
   cursor.execute("CREATE TABLE bench_fts_heap AS TABLE netflix_master;")
   cursor.execute(sql.SQL(fts_prep_sql).format(table=sql.Identifier("bench_fts_heap")))

   #table 2 : GIN Index (the gold standard)
   cursor.execute("DROP TABLE IF EXISTS bench_fts_gin")
   cursor.execute("CREATE TABLE bench_fts_gin AS TABLE netflix_master;")
   cursor.execute(sql.SQL(fts_prep_sql).format(table=sql.Identifier("bench_fts_gin")))
   cursor.execute("CREATE INDEX idx_fts_gin ON bench_fts_gin USING GIN (search_vector);")



   """
   We need to implement the creation of vector indexes here with the help of vector embeddings BUT we dont have a way to create them right now so will follow up on this later   
   """


   """
   Lets go ahead with the creation of array search indexing here
   """

   print("Setting up array search tables...")
   # Table 1: No Index (Verify how slow ANY() is without GIN)
   cursor.execute("DROP TABLE IF EXISTS bench_array_heap;")
   cursor.execute("CREATE TABLE bench_array_heap AS TABLE netflix_master;")

   # Table 2: GIN Index on Arrays
   cursor.execute("DROP TABLE IF EXISTS bench_array_gin;")
   cursor.execute("CREATE TABLE bench_array_gin AS TABLE netflix_master;")
   # "cast" is our array column of choice
   cursor.execute("""
       CREATE INDEX idx_array_gin ON bench_array_gin 
       USING GIN ("cast");
   """)

   #with this we will test array pattern search against GIN index on arrays 
   print("   - Creating FTS version of Cast list...")
   cursor.execute("DROP TABLE IF EXISTS bench_array_fts;")
   cursor.execute("CREATE TABLE bench_array_fts AS TABLE netflix_master;")

   cursor.execute("""
        ALTER TABLE bench_array_fts ADD COLUMN cast_vector tsvector;
        UPDATE bench_array_fts 
        SET cast_vector = to_tsvector('english', array_to_string("cast", ' '));
    """)


   cursor.execute("""
        CREATE INDEX idx_cast_fts_gin 
        ON bench_cast_fts 
        USING GIN (cast_vector);
    """)

    #I really need to understand the creation of GIN  index for these columns


   print("   - Creating Text/Trigram version of Cast list...")
   cursor.execute("DROP TABLE IF EXISTS bench_cast_trgm;")
   cursor.execute("CREATE TABLE bench_cast_trgm AS TABLE netflix_master;")
    
   cursor.execute("""
        ALTER TABLE bench_cast_trgm ADD COLUMN cast_text TEXT;
        UPDATE bench_cast_trgm 
        SET cast_text = array_to_string("cast", ', ');
    """)
   cursor.execute("""
        CREATE INDEX idx_cast_trgm_gin 
        ON bench_cast_trgm 
        USING GIN (cast_text gin_trgm_ops);
    """)


   """
   Setting up Vector search 
   """

   print("Setting up vector search tables...")

   print("loading the model")
   model = SentenceTransformer('all-MiniLM-L6-v2')
   vector_dims = 384

   cursor.execute("DROP TABLE IF EXISTS bench_vector_heap;")
   cursor.execute("CREATE TABLE bench_vector_heap AS TABLE netflix_master;")
   cursor.execute(f"ALTER TABLE bench_vector_heap ADD COLUMN embedding vector({vector_dims})")

   print("fetch the data we need to create the embeddings on")
   cursor.execute("SELECT show_id , title , description FROM netflix_master")
   rows = cursor.fetchall()
   

   batch_size = 500
   total_rows = len(rows)
   print(f"Generating embeddings for {total_rows} rows in batches of {batch_size}...")

   update_sql = """
   UPDATE bench_vector_heap as t 
   SET embedding = v.val::vector
   FROM (VALUES %s) as v(id,val)
   WHERE t.show_id = v.id;
   """

   for i in range(0 , total_rows , batch_size):

      texts = [f"{r[1] or ''}: {r[2] or ''}" for r in batch]
      ids = {r[0] for r in batch}

      embeddings = model.encode(texts)
      
      update_data = [(ids[j] , embeddings[j].tolist()) for j in range(len(batch))]

      execute_values(cursor , update_sql , update_data , page_size=batch_size);

      if(i + batch_size % 2000) == 0:
         print(f"processed {i + batch_size}/{total_rows} rows")
       
   