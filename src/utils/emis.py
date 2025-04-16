import csv
import os
import network_util as net
import pandas as pd

from datetime import datetime

#Global variables
date_data_start = False
date_data_end = None

#Unzip the EMIS zip file
def extract_files():
    tmp_dir = net.manage_temp(dir="./data/", mode="+")

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

def process_emis_datafile(data_file):

    #Check if the date of the data has been derived yet
    global date_data_start
    global date_data_end
    if not(date_data_start):
        derive_date(data_file)

    #Determine header row
    first_row = get_emis_table_start(data_file)

    #Load the file
    df = pd.read_csv(data_file, encoding='utf-8', skiprows=first_row)

    #Rename the % column
    df.rename(columns={"%":"Percentage"}, inplace=True)

    #Trim the unused columns
    df.drop("Status", axis=1, inplace=True)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    #Add the date columns
    df["Start_Date"] = date_data_start
    df["End_Date"] = date_data_end

    print(df.head())

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
                df = process_emis_datafile(full_path)

               

def emis_manager(zip=True):
    if zip:
        data_dir = extract_files()

# Files I care about (others are unused)
## Cancer/CAN004
## Cancer/CAN005
## Electronic safety netting audit/Pts with...
## FIT/...before Ref.csv
## social prescribing refferal/Social prescriber referrals

emis_cancer([], "./data/tmp/", file_ids=["004", "005"])