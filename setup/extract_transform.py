# For this ETL pipeline we will be following these steps:
#     clean the csv cast array by stripping them of whitespaces and removing commas to store them individually 
#     then we start by loading the dates -> standardize dates -> convert the strings to lists ->
#     fill the NaN with NULL -> handle missing values by converting them to NULL -> de duplicate -> convert to tuples

import pandas as pd

def clean_csv_array(value):
    if pd.isna(value) or value == "":
        return None
    return [x.strip() for x in value.split(',')]


def load_and_clean_data(filepath):
    print(f"loading and transforming data from {filepath}....")
    
    df  = pd.read_csv(filepath)
    
    df['date_added'] = pd.to_datetime(df['date_added'], errors = 'coerce')
    
    list_columns = ['cast', 'country', 'listed_in']
    
    for column in list_columns:
        df[column] = df[column].apply(clean_csv_array)
    
    df = df.convert_dtypes()
    
    df.drop_duplicates(subset=['show_id'], keep='first' , inplace=True)
    
    records = list(df.itertuples(index=False , name=None))
    
    print(f"Transformation complete. Prepared {len(records)} rows")
    return records
    