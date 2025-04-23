import requests
import fingertips_py as ftp

#List of fingertip metric ids
# 91355 - Emergency admission with cancer
# 91357 - Number of other (non-emergency) presentables

#Get indicator metadata
def get_indicator_metadata(indicator_ids):
    #Convert to array if singular id given
    if type(indicator_ids) == int or type(indicator_ids) == str:
        indicator_ids = [indicator_ids]
    
    #Fetch the metadata
    return ftp.metadata.get_metadata_for_indicator_as_dataframe(
        indicator_ids)

#Get data for the given indicator
def get_data_for_indicator(indicator_id):
    pass

def indicator_manager(settings):
    #Get indicators
    indicators = settings["pop"]["indicators"]

    #Get metadata on these indicators
    metadata = get_indicator_metadata(indicators)

    #Determine which indicators has new data


    print(metadata)

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