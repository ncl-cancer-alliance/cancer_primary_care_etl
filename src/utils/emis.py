#Standard Modules
import csv
import os
import pandas as pd

from datetime import date, datetime

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

    #Build overlapping data maintenance query
    ##Phase out in future versions
    query_base = f"DELETE FROM [{schema}].[{table}] " 

    line = ""
    for idx, col in enumerate(id_cols):

        if idx == 0:
            line += "WHERE "
        else:
            line += "AND "

        #Get data from df
        unique_vals = list(data[col].unique())

        #Check if this is a date column and convert to string
        if isinstance(unique_vals[0], date):
            unique_vals = [x.strftime("%Y-%m-%d") for x in unique_vals]

        if len(unique_vals) == 1:
            line += f"[{col}] = '{unique_vals[0]}' "
        else:
            line += f"[{col}] IN '{', '.join(unique_vals)}' "

    query = query_base + line 

    db.execute_query(engine, query)
  
    #Basic upload code
    data.to_sql(name=table, con=engine, schema=schema,
                if_exists="append", index=False, chunksize=100, method="multi")

#Custom Dataset Processing Functions
#These are data manipulation that is specific to the datasets
####################################

#Parametes = Indicator Name
def processing_ccr(df, parameters):

    global date_data_start

    #Convert date into "MMM yy"
    date_string = date_data_start.strftime("%b %y")

    df["Indicator"] = parameters + " - " + date_string

    return df

#Parameters = None
def processing_fit(df, parameters):

    df["Date_Type"] = "Monthly"

    return df

#Parameters = Indicator Name
def processing_spr(df, parameters):
    #Add Indicator Name to data
    df["Indicator_Name"] = parameters

    #Rename columns to match the Pop Health table schematics
    rename_map = {
        "Organisation":"Area_Name",
        "CDB":"Area_Code",
        "Population_Count":"Value",
        "Parent":"Denominator",
        "Start_Date":"Time_period_Sortable",
        "End_Date":"Date_updated"
    }
    df.rename(columns=rename_map, inplace=True)

    unused_cols = ["Percentage", "Males", "Females", "Excluded", "Status"]

    df.drop(labels=unused_cols, axis=1, inplace=True)

    return df

####################################

#Custom File Identifiers
####################################
def file_id_ccr(settings, ds, data_files):
    target_files = []

    base_prefix = settings["ds"][ds]["metric_id_prefix"]
    metrics_ids = settings["ds"][ds]["metric_ids"]
    target_metrics =  [base_prefix + x for x in metrics_ids]

    for data_file in data_files:
        if data_file[0:6] in target_metrics:
            target_files.append(data_file)

    return target_files

def file_id_esafety(settings, ds, data_files):
    return [x for x in data_files if "esafety" in x]

def file_id_fit(settings, ds, data_files):
    return [x for x in data_files if "before Ref" in x]

def file_id_spr(settings, ds, data_files):
    return [x for x in data_files if "Social" in x]
####################################

#Load Custom Parameters
####################################
def custom_parameters_ccr(settings, ds, data_file):
    #Load metric id information
    metric_id_map = settings["ds"][ds]["metric_id_map"]

    #Get indicator name using the file id and the metric id map
    indicator_name = metric_id_map[data_file[3:6]]

    return indicator_name

def custom_parameters_esafety(settings, ds, data_file):
    return settings["ds"][ds]["esafety_indicator_name"]

def custom_parameters_spr(settings, ds, data_file):
    return settings["ds"]["SPR"]["indicator_name"]

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

    #Remove whitespace in column names
    df.columns = df.columns.str.replace('\n', ' ', regex=False)
    df.columns = df.columns.str.strip().str.replace(' ', '_')

    #Remove the Total row from the data
    df = df[df["Organisation"] != "Total"]

    #Apply dataset specific processing (if any)
    if custom_processing:
        df = custom_processing(df, custom_parameters)

    #Upload the new data (Replace this in the future with more modular solution)
    engine = db.db_connect(settings["db_dsn"], settings["db_database"])
    upload_data(engine, df, 
                settings["ds"][ds]["db_dest_table"], settings["db_dest_schema"],
                id_cols=settings["ds"][ds]["id_cols"])

    return df

#Function to manage which files to process
def emis_file_manager(settings, parent_dir, ds, 
                func_file_id=False, 
                func_custom_processing=False, func_custom_params=False):
    
    #Find the dataset subdirectory
    ds_name = settings["ds"][ds]["name"]
    subdir_str_ids = settings["ds"][ds]["subdir_substrings"]
    data_dir = find_data_subdir(parent_dir, 
                                substrings=subdir_str_ids, name=ds_name)

    #If the directory was found
    if data_dir:
        #Handle each file indivdually
        data_files = os.listdir(parent_dir + data_dir)

        #Determine which files in the directory should be processed
        if func_file_id:
            target_files = func_file_id(settings, ds, data_files)
        else:
            target_files = data_files
        
        #Process each file iteratively
        for data_file in target_files:
            full_path = parent_dir + data_dir + "/" + data_file

            #Get dataset custom parameters
            if func_custom_params:
                custom_parameters = func_custom_params(settings, ds, data_file)
            else:
                custom_parameters = False

            process_emis_datafile(full_path, ds,
                                    custom_processing=func_custom_processing,
                                    custom_parameters=custom_parameters)          

#Parent function to handle all pipelines and data loading
def emis_dataset_manager(settings, zip=True):

    #Extract the data
    if zip:
        data_dir = extract_files()
    else:
        data_dir = "./data/emis/"

    pipelines = settings["emis_pipelines"]

    runtime_pipelines = [x for x in pipelines if settings["ds"][x]["run"]]

    #Run the EMIS pipelines
    if runtime_pipelines:
        print("Running the EMIS Pipeline:")

    for ds in runtime_pipelines:
        print(f"-> {ds} Pipeline")

        #Custom processing functions
        custom_funcs = settings["ds"][ds]["func"]

        emis_file_manager(
            settings,
            data_dir,
            ds,
            func_file_id=custom_funcs["file_id"],
            func_custom_processing=custom_funcs["custom_processing"],
            func_custom_params=custom_funcs["custom_parameters"]
        )

    #Clean up directory
    if zip:
        net.manage_temp()

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
    "emis_pipelines": ["CCR", "eSafety", "FIT", "SPR"],
    "ds":{
        "CCR":{"run":True,
               "name":"Cancer Care Reviews",
               "subdir_substrings":["Cancer"],
               "db_dest_table":"Monthly_CCR_Safety_Netting_Test",
               "metric_id_prefix": "CAN",
               "metric_ids": ["004", "005"],
               "metric_id_map":{
                   "004":"CAN004 - Cancer Care Review within 12 months",
                   "005":"CAN005 - Cancer support offered within 3 months"},
               "id_cols": ["Start_Date", "Indicator"],
               "func":{
                   "file_id": file_id_ccr,
                   "custom_processing": processing_ccr,
                   "custom_parameters": custom_parameters_ccr
               }
        },
        "eSafety":{
            "run":True,
            "name":"e-Safety Netting",
            "subdir_substrings":["netting"],
            "db_dest_table":"Monthly_CCR_Safety_Netting_Test",
            "esafety_indicator_name":"USC referrals safety netted via e-safety netting tool",
            "id_cols": ["Start_Date", "Indicator"],
            "func":{
                   "file_id": file_id_esafety,
                   "custom_processing": processing_ccr,
                   "custom_parameters": custom_parameters_esafety
               }
        },
        "FIT":{"run":True,
               "name":"FIT",
               "subdir_substrings":["FIT"],
               "db_dest_table":"Monthly_FIT_NCL_Test",
               "id_cols": ["Start_Date", "Date_Type"],
               "func":{
                   "file_id": file_id_fit,
                   "custom_processing": processing_fit,
                   "custom_parameters": False
                   }
        },
        "SPR":{"run":True,
               "name":"Social Prescribing Referrals",
               "subdir_substrings":["social", "prescrib"],
               "db_dest_table":"Monthly_Population_Health_Test",
               "indicator_name":"No. of Social Prescribing referrals made within the last 12 months",
               "id_cols": ["Date_updated", "Indicator_Name"],
               "func":{
                   "file_id": file_id_spr,
                   "custom_processing": processing_spr,
                   "custom_parameters": custom_parameters_spr
                   }
        }
    }
}
emis_dataset_manager(settings)