#Standard Modules
import csv
import os
import pandas as pd

from datetime import datetime

#Util Modules
import database_util as db
import network_util as net

#Global variables
date_data_start = False
date_data_end = None

#Unzip the EMIS zip file
def extract_files():
    tmp_dir = net.manage_temp(mode="+")

    net.unzip_file(
        "Cancer,FIT,social prescribing & electronic safety netting audit.zip", 
        dest_dir=tmp_dir)
    
    return tmp_dir

#Find the subdirectory for a given dataset
def find_data_subdir(parent_dir, substrings, name, exact=False, case=False):
    #Get all subdirs
    child_dirs = next(os.walk(parent_dir))[1]

    #Look for matching subdirectories
    matching_dirs = []
    for subdir in child_dirs:
        if all(sub in subdir for sub in substrings):
            matching_dirs.append(subdir)

    if len(matching_dirs) == 1:
        return matching_dirs[0]
    elif len(matching_dirs) == 0:
        print(f"Warning: No {name} directories found in {parent_dir}")
    else:
        print(f"Warning: Multiple {name} directories found in {parent_dir}")

    return False

#Derive the date of the data and set the global variable
def derive_date(data_file, keyword_row="Last Run", keyword_col="Relative Date"):

    global date_data_start
    global date_data_end

    #Open the file and read until the date row in the metadata is found
    with open(data_file, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            # Date metadata row found
            if row and row[0].strip().lower() == keyword_row.lower():
                #Look for the Relative Date in the row
                for idx, col in enumerate(row):
                    if col and col.strip().lower() == keyword_col.lower():
                        #Convert the Relative Date in the file into a date var
                        rel_date = datetime.strptime(
                            row[idx+1], '%d-%b-%Y').date()
                        
                        #Update the date data start and end variables
                        date_data_start = rel_date
                        date_data_end = datetime(
                            rel_date.year + int(rel_date.month / 12), #Year
                            ((rel_date.month % 12) + 1), #Month
                            1).date() #Day

                        return True
    
    print("Warning: The date executed could not be found in the file")
    return False

#Look for where the tabular data starts
def get_emis_table_start(data_file, keyword="Organisation"):
    #Open the file and read until the column headings are found
    with open(data_file, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for idx, row in enumerate(reader):
            # Header row found
            if row and row[0].strip().lower() == keyword.lower():
                return idx
    
    print("Warning: The header row could not be found in the file")
    return False

#Custom data upload function
## Replace if I can make the destination tables more generic
def upload_data(engine, data, table, schema, id_cols=["Start_Date"]):

    #Delete existing data
    global date_data_start

    date_str = date_data_start.strftime("%Y-%m-%d")

    #Build overlapping data maintenance query
    ##Phase out in future versions
    query_base = f"DELETE FROM [{schema}].[{table}] " 

    line = ""
    for idx, col in enumerate(id_cols):

        if idx == 0:
            line += "WHERE "
        else:
            line += "AND "

        if col == "Start_Date":
            line += f"[Start_Date] = '{date_str}' "
        else:
            #Get data from df
            unique_vals = list(data[col].unique())
            if len(unique_vals) == 1:
                line += f"[{col}] = '{unique_vals[0]}'"
            else:
                line += f"[{col}] IN '{', '.join(unique_vals)}'"

    query = query_base + line 

    db.execute_query(engine, query)
  
    #Basic upload code
    data.to_sql(name=table, con=engine, schema=schema,
                if_exists="append", index=False, chunksize=100, method="multi")


#Custom Dataset Processing Functions
####################################

#Parametes = indicator_name
def ccr_processing(df, parameters):

    global date_data_start

    #Convert date into "MMM yy"
    date_string = date_data_start.strftime("%b %y")

    df["Indicator"] = parameters + " - " + date_string

    return df

####################################

#Function to process emis data files
def process_emis_datafile(data_file, ds, 
                          custom_processing=False, custom_parameters=None):

    #Check if the date of the data has been derived yet
    global date_data_start
    global date_data_end
    if not(date_data_start):
        derive_date(data_file)

    #Determine header row
    first_row = get_emis_table_start(data_file)

    #Load the file
    try:
        df = pd.read_csv(data_file, encoding='utf-8', skiprows=first_row)
    except:
        df = pd.read_csv(data_file, encoding='utf-16', skiprows=first_row)

    #Rename the % column
    df.rename(columns={"%":"Percentage"}, inplace=True)

    #Trim the unused columns
    #df.drop("Status", axis=1, inplace=True)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    #Add the date columns
    df["Start_Date"] = date_data_start
    df["End_Date"] = date_data_end

    #Apply dataset specific processing (if any)
    if custom_processing:
        df = custom_processing(df, custom_parameters)

    #Upload the new data (Replace this in the future with more modular solution)
    engine = db.db_connect(settings["db_dsn"], settings["db_database"])
    upload_data(engine, df, 
                settings["ds"][ds]["db_dest_table"], settings["db_dest_schema"],
                id_cols=["Start_Date", "Indicator"])

    return df

#Function to process the Cancer Care Review files
def emis_cancer(settings, parent_dir, file_ids=["004", "005"]):
    #Find Cancer Care Review directory
    data_dir = find_data_subdir(parent_dir, substrings=["Cancer"], name="Cancer Care Review")

    #If the directory was found
    if data_dir:
        #Handle each file indivdually
        base_prefix = "CAN"
        data_files = os.listdir(parent_dir + data_dir)
        
        for data_file in data_files:
            #Check if this data_file is one we want to process
            if data_file[0:3] == base_prefix and data_file[3:6] in file_ids:
                full_path = parent_dir + data_dir + "/" + data_file

                #Get indicator name from settings
                indicator_name = settings["ccr_map_id"][data_file[3:6]]

                process_emis_datafile(full_path, "CCR",
                                      custom_processing=ccr_processing,
                                      custom_parameters=indicator_name)          

#Function to process the e-Safety Netting files
def emis_esafety(settings, parent_dir):
    #Find e-safety netting directory
    data_dir = find_data_subdir(parent_dir, substrings=["netting"], 
                                name="e-Safety Netting")

    #If the directory was found
    if data_dir:
        #Handle each file indivdually
        data_files = os.listdir(parent_dir + data_dir)
        
        for data_file in data_files:
            #Check if this data_file is one we want to process
            if "esafety" in data_file:
                full_path = parent_dir + data_dir + "/" + data_file

                #Get indicator name from settings
                indicator_name = settings["ds"]["CCR"]["esafety_indicator_name"]

                process_emis_datafile(full_path, ds="CCR", 
                                      custom_processing=ccr_processing,
                                      custom_parameters=indicator_name)   

#USC referrals safety netted via e-safety netting tool

def emis_manager(settings, zip=True):

    #Extract the data
    if zip:
        data_dir = extract_files()
    else:
        data_dir = "./data/emis/"

    #Run the EMIS pipelines
    emis_cancer(settings, data_dir)
    emis_esafety(settings, data_dir)

    #Clean up directory
    if zip:
        #net.manage_temp()
        pass

# Files I care about (others are unused)
## Cancer/CAN004
## Cancer/CAN005
## Electronic safety netting audit/Pts with...
## FIT/...before Ref.csv
## social prescribing refferal/Social prescriber referrals

settings = {
    "db_dsn": "SANDPIT",
    "db_database": "Data_Lab_NCL_Dev",
    "db_dest_schema": "GrahamR",
    "ccr_map_id":
        {"004":"Cancer Care Review within 12 months",
         "005":"CAN005 - Cancer support offered within 3 months"},
    "ds":{
        "CCR":{"db_dest_table":"Monthly_CCR_Safety_Netting_Test",
               "esafety_indicator_name":"USC referrals safety netted via e-safety netting tool"}
    }
}
emis_manager(settings)