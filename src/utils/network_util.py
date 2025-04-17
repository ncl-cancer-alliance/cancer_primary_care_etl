import pandas as pd
import os
import shutil
import zipfile

#Creates a tmp folder for files during execution. 
#Deletes the tmp folder when recalled
def manage_temp (dir="./data/", mode=None):

    tmp_dir = dir + "tmp/"

    #Check if the tmp folder already exists
    if not(os.path.exists(tmp_dir)) and mode != "-":
        #Create the tmp folder
        os.makedirs(tmp_dir)
    elif mode != "+":
        #Delete the tmp folder
        shutil.rmtree(tmp_dir, ignore_errors=True)
    else:
        if mode == "+":
            print(f"Warning: {tmp_dir} already exists and cannot be created.")
        if mode == "-":
            print(f"Warning: {tmp_dir} does not exist and cannot be deleted.")

    return tmp_dir

#Unzip a file and save the contents in a specified location
def unzip_file(zip_file, source_dir="./data/", dest_dir=False):

    #Build the full file path
    file_path = source_dir + zip_file

    #Set the destination directory
    if not dest_dir:
        dest_dir = source_dir

    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)

#Function to convert excel worksheets into csv files
def convert_csv(file, src_ext):
    #Save the existing file as a csv
    df = pd.read_excel(file, engine='openpyxl')
    csv_filename = ".".join(file.split(".")[:-1]) + ".csv"
    df.to_csv(csv_filename)

    #Delete the original file
    os.remove(file)

    return csv_filename