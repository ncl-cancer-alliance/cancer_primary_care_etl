import pandas as pd
import os

import utils.database_util as db

def load_all_files(data_dir):
    """
    Function to load all files in a directory and return as a df
    data_dir: Directory containing data
    Returns:
        - df: Dataframe containing the loaded data
    """

    all_files = os.listdir(data_dir)
    csv_files = [x for x in all_files if ".csv" in x]
    
    #Load first value into the collated dataframe
    dfs = pd.read_csv(os.path.join(data_dir, csv_files[0]), 
                      encoding="utf-16", delimiter="\t")

    #Concat the remaining dataframes
    for csv_file in csv_files[1:]:
        df = pd.read_csv(os.path.join(data_dir, csv_file), 
                         encoding="utf-16", delimiter="\t")
        
        dfs = pd.concat([dfs, df], ignore_index=True)

    return dfs

def process_national(df):
    """
    Handles the transformation of national screening data
    df: Raw screening data, expects regional level data
    Returns:
        - df: Transformed data
    """

    #Process England data
    pre_agg_cols = ["Programme", "Month_of_Date",
       "Cohort_Age_Range", "Cohort_Description", "Denominator_Name", "Acceptable", "Achievable",
       "Denominator", "Numerator"]
    df_e = df[pre_agg_cols]

    #Convert numeric columns to numeric values
    df_e['Denominator'] = pd.to_numeric(
        df_e['Denominator'].str.replace(',', ''), errors='coerce')
    df_e['Numerator'] = pd.to_numeric(
        df_e['Numerator'].str.replace(',', ''), errors='coerce')

    #Aggregate figures together for England overall
    df_e = df_e.groupby(by=pre_agg_cols[:-2], as_index=False).sum()

    #Add additional information columns
    df_e["Organisation_Code"] = "ENG"
    df_e["Organisation_Name"] = "England"
    df_e["Geo_Region"] = "England"
    df_e["Performance"] = df_e['Numerator'] / df_e['Denominator']
    df_e["Performance"] = df_e["Performance"].apply(
        lambda x: '{:.1%}'.format(x) if pd.notnull(x) else x
    )

    #Seperate out London as it's own dataframe
    df_l = df[df["Organisation_Name"] == "London"]

    #Convert numeric columns to numeric values
    df_l['Denominator'] = pd.to_numeric(
        df_l['Denominator'].str.replace(',', ''), errors='coerce')
    df_l['Numerator'] = pd.to_numeric(
        df_l['Numerator'].str.replace(',', ''), errors='coerce')
    
    #Add additional information columns
    df_l["Organisation_Code"] = "LON"
    df_l["Geo_Region"] = "London"

    df = pd.concat([df_e, df_l])

    return df

def process_screening_data(settings, data_dir, file_type):
    """
    Function to process screening data files in a given directory
    data_dir: Directory containing data
    file_type: Either "national" or "local"
    Returns:
        None
    """

    #Load all data from the data directory
    df = load_all_files(data_dir)

    #Clean the column names
    df.columns = df.columns.str.replace(
        '\n', ' ', regex=False).str.strip().str.replace(' ', '_')
    
    print(df.columns)

    #Process national and local data seperately
    if file_type == "national":
        df = process_national(df)
    elif file_type == "local":
        pass
    else:
        raise Exception (f"File type {file_type} not recognised")
    
    #Upload the new data (Replace this in the future with more modular solution)
    engine = db.db_connect(settings["db_dsn"], settings["db_database"])
    table = settings["screening"]["db_dest_table"]
    schema = settings["db_dest_schema"]

    df.to_sql(name=table, con=engine, schema=schema,
                if_exists="append", index=False, chunksize=100, method="multi")