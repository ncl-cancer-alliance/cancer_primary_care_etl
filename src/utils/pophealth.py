import requests
import fingertips_py as ftp
import pandas as pd

from io import BytesIO
from sqlalchemy import create_engine, MetaData, text, insert
from urllib.parse import urlencode

#Util Modules
try:
    import utils.database_util as db
    import utils.network_util as net
except:
    import database_util as db
    import network_util as net

#Get indicator metadata
def get_indicator_metadata(indicator_ids):
    #Convert to array if singular id given
    if type(indicator_ids) == int or type(indicator_ids) == str:
        indicator_ids = [indicator_ids]
    
    #Fetch the metadata
    return ftp.metadata.get_metadata_for_indicator_as_dataframe(
        indicator_ids)

#Function to check the metadata of data currently in the database and determine 
# which indicators need to be updated
def get_new_data_indicators(settings, indicators, remote_metadata):
    #Load query
    #Set up Database Connection
    dsn = settings["db_dsn"]
    sql_database = settings["db_database"]

    #Connect to the database
    engine = db.db_connect(dsn, sql_database)
    with engine.connect() as con:
        
        #Load the ICS Lookup sql script and store the results
        with open(settings["pop"]["query_local_metadata"]) as file:
            sfw_query = text(file.read())
            df_meta = pd.read_sql_query(sfw_query, con)

    #Only include selected metrics
    df_meta = df_meta[df_meta["indicator_id"].isin(
        [str(x) for x in indicators])]

    #Format remote metadata in terms of the local metadata
    remote_dates = remote_metadata.copy()
    #Rename columns
    remote_dates.rename(
        columns={
            "Indicator ID":"indicator_id", 
            "Date updated": "date_updated"},
        inplace=True)
    #Only keep relevant columns
    remote_dates = remote_dates[["indicator_id", "date_updated"]]
    #Convert ids to strings
    remote_dates["indicator_id"] = remote_dates["indicator_id"].astype(str)
    #Convert date format
    remote_dates["date_updated"] = pd.to_datetime(
        remote_dates["date_updated"], format="%d/%m/%Y")

    #If force update is enable then rerun all indicators even if up to date
    if settings["pop"]["force_update"]:
        new_data_indicators = list(remote_dates["indicator_id"].unique())
    else:
        #Merge the local and remote
        merged = remote_dates.merge(
            df_meta, on="indicator_id", 
            suffixes=("_remote", "_local"),
            how="left")

        #Filter out rows where the remote date matches the local date
        merged = merged[(
            merged["date_updated_local"] != merged["date_updated_remote"])]
        
        new_data_indicators = list(merged["indicator_id"].unique())

    return new_data_indicators

#Get data for the given indicator
#Area Type ID: 7-GP Practices, 221-ICBs 
#(No data for regions for many metrics so use ICB and aggregate)
#Parent Area Type ID: 66-ICBs [15 is the default used by ftp if unspecified]
def get_data_for_indicators(indicators, area_type_id=7, 
                            parent_area_type_id=15,
                            read_chunks=8192):
    
    #Set up url and end point
    base_url = 'https://fingertipsws.phe.org.uk/api/'
    endpoint = 'all_data/csv/by_indicator_id'

    #Set up parameters for the request
    params = {
        'indicator_ids': ','.join(indicators),
        'child_area_type_id': area_type_id,
        'parent_area_type_id': parent_area_type_id,
        'filter_by_area_codes': False
    }

    #Make the GET API request
    full_url = f"{base_url}{endpoint}?{urlencode(params)}"
    response = requests.get(full_url)
    response.raise_for_status()

    #Save the response if successful
    chunk_iter = pd.read_csv(BytesIO(response.content), chunksize=read_chunks)

    # Combine all chunks into one DataFrame
    df = pd.concat(chunk_iter, ignore_index=True)
    
    return df

#Format the data (i.e. column names and included columns)
def format_indicator_data(settings, df):

    #Remove whitespace in column names
    df.columns = df.columns.str.replace('\n', ' ', regex=False)
    df.columns = df.columns.str.strip().str.replace(' ', '_')

    df = df[["Indicator_ID", "Indicator_Name", "Area_Code", "Area_Name", 
             "Value","Denominator", "Time_period_Sortable", "Time_period_range",
             ]]
    
    return df

#Upload the Population Health Data
def upload_pop_data(settings, df, dest_table, indicators):

    #Get schema
    dest_schema = settings["db_dest_schema"]

    #Set up the engine and db connection
    engine = db.db_connect(settings["db_dsn"], settings["db_database"])

    #Delete overlapping data
    query = (f"DELETE FROM [{dest_schema}].[{dest_table}]" +
                 f" WHERE [Indicator_Id] IN ({", ".join(indicators)})")
    
    db.execute_query(engine, query)

    #Upload the new data
    #Basic upload code
    df.to_sql(name=dest_table, con=engine, schema=dest_schema,
                if_exists="append", index=False, chunksize=100, method="multi")

#Function to handle the Extraction and Transformation of Practice level data
def et_practice(settings, indicators):

    #Extract the data
    df_prac = get_data_for_indicators(indicators, area_type_id=7, 
                                      parent_area_type_id=66)
    #Transform the data
    #Filter to NCL Practices
    df_prac = (
        df_prac[df_prac["Parent Code"] == settings["pop"]["area_code_ncl"]])
    
    #Format the output as standard for the indicator data
    df_prac = format_indicator_data(settings, df_prac)

    return df_prac

#Wrapper function to manage the population health pipeline
def indicator_manager(settings):
    #Get indicators
    indicators = settings["pop"]["indicators"]

    #Get metadata on these indicators
    metadata = get_indicator_metadata(indicators)

    #Determine which indicators has new data
    target_indicators = get_new_data_indicators(settings, indicators, metadata)

    #Only process data is there is at least 1 target indicator
    if target_indicators:

        #Practice Level Data
        df_prac = et_practice(settings, target_indicators)

        #Upload the output
        upload_pop_data(
            settings, df_prac, 
            dest_table=settings["pop"]["db_dest_table_practice"],
            indicators=target_indicators)
    
    ## In a later version, add benchmarking and metadata table

#res = get_indicator_metadata(['91355', '91357', '276', '91280', '91845', '92588', '93553', '91337','93275'])

# Set list of indicators
# Check for new data
# Download data
# Filter to NCL, get the London and England rows as well
# Option for all data or latest datapoint (Is the date updated specific per datapoint?)
# Upload to the table
# Seperate table for the benchmarking data