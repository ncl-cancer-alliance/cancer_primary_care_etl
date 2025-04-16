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