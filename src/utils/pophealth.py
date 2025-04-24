import requests
import fingertips_py as ftp

from sqlalchemy import create_engine, MetaData, text, insert

from runtime import *

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
def get_data_for_indicators(indicators, area_type_id=7):
    df = ftp.get_data_by_indicator_ids(
        indicators, area_type_id=7)
    
    print(df.head(10)[["Area Code", "Area Name"]])

    df.to_csv("testout.csv")
    
    return df

#Wrapper function to manage the population health pipeline
def indicator_manager(settings):
    #Get indicators
    indicators = settings["pop"]["indicators"]

    #Get metadata on these indicators
    metadata = get_indicator_metadata(indicators)

    #Determine which indicators has new data
    target_indicators = get_new_data_indicators(settings, indicators, metadata)

    #Download data
    df = get_data_for_indicators(target_indicators)


settings = build_settings()
indicator_manager(settings)

#res = get_indicator_metadata(['91355', '91357', '276', '91280', '91845', '92588', '93553', '91337','93275'])

# Set list of indicators
# Check for new data
# Download data
# Filter to NCL, get the London and England rows as well
# Option for all data or latest datapoint (Is the date updated specific per datapoint?)
# Upload to the table
# Seperate table for the benchmarking data