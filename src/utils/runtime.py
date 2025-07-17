# This file contains the code to manage runtime settings as well as build the 
# settings dictionary object

import toml
from dotenv import load_dotenv
from os import getenv

#Try Except to allow for debugging
try:   
    from utils.emis import *
except:
    from emis import *

def build_settings():
    #Load env settings
    load_dotenv(override=True)

    #Load toml settings from config
    config = toml.load("./config.toml")

    #Load which EMIS datasets are selected
    emis_datasets = config["emis"]["pipelines"]

    #List of all pop health metrics
    pop_all_indicators = config["pop"]["all_indicators"]

    #Determine which pop health indicators are pulled based on the .env settings
    if getenv(f"POP_RUNALL") != "False":
        #If POP_RUNALL is True then also include indicators set to Default
        pop_indicator_criteria = ["True", "Default"]
    else:
        #If POP_RUNALL is False then only include indicators set to True
        pop_indicator_criteria = ["True"]

    settings = {
        "base_dir": config["base"]["data_dir"],
        "db_dsn": config["base"]["db_dsn"],
        "db_database": config["base"]["db_database"],
        "db_dest_schema": config["base"]["db_schema"],

        "upload": (getenv(f"RUN_UPLOAD") != "False"),

        "emis":{
            "run": (getenv(f"RUN_EMIS") != "False"),
            "zip": (getenv(f"EMIS_ZIPFILE") != "False"),
            "data_dir": config["emis"]["data_dir"],
            "pipelines":(
                [x for x in emis_datasets if getenv(f"EMIS_{x}") != "False"])
        },
        "pop":{
            "run": (getenv(f"RUN_POP") != "False"),
            "indicators": ([x for x in pop_all_indicators if (
                getenv(f"POP_{x}") in pop_indicator_criteria)]),
            "area_code_ncl": config["pop"]["area_code_ncl"],
            "area_codes_london": config["pop"]["area_codes_london"],
            "db_dest_table_practice": config["pop"]["db_dest_table_practice"],
            "db_dest_table_benchmark": config["pop"]["db_dest_table_benchmark"],
            "db_dest_table_metadata": config["pop"]["db_dest_table_metadata"],
            "query_local_metadata": config["pop"]["query_local_metadata"],
            "detailed_logging": (getenv(f"POP_DETAILED_LOGGING") != "False"),
            "force_update": (getenv(f"POP_FORCE") != "False")
        },
        "ds":{
            "CCR":{
                #Common variables
                "name": config["emis"]["ccr"]["name"],
                "subdir_substrings":config["emis"]["ccr"]["subdir_substrings"],
                "db_dest_table": config["emis"]["ccr"]["db_dest_table"],
                "id_cols": config["emis"]["ccr"]["data_id_cols"],
                #Dataset variables
                "metric_id_prefix": config["emis"]["ccr"]["metric_id_prefix"],
                "metric_ids": config["emis"]["ccr"]["metric_ids"],
                "metric_id_map":{
                    "004": config["emis"]["ccr"]["metric_id_map"]["004"],
                    "005": config["emis"]["ccr"]["metric_id_map"]["005"]},
                #Functions
                "func":{
                   "file_id": file_id_ccr,
                   "custom_processing": processing_ccr,
                   "custom_parameters": custom_parameters_ccr},
            },
            "eSafety":{
                #Common variables
                "name": config["emis"]["esafety"]["name"],
                "subdir_substrings":config["emis"]["esafety"]["subdir_substrings"],
                "db_dest_table": config["emis"]["esafety"]["db_dest_table"],
                "id_cols": config["emis"]["esafety"]["data_id_cols"],
                #Dataset variables
                "esafety_indicator_name": config["emis"]["esafety"]["esafety_indicator_name"],
                #Functions
                "func":{
                   "file_id": file_id_esafety,
                   "custom_processing": processing_ccr,
                   "custom_parameters": custom_parameters_esafety},
            },
            "FIT":{
                #Common variables
                "name": config["emis"]["fit"]["name"],
                "subdir_substrings":config["emis"]["fit"]["subdir_substrings"],
                "db_dest_table": config["emis"]["fit"]["db_dest_table"],
                "id_cols": config["emis"]["fit"]["data_id_cols"],
                #Functions
                "func":{
                   "file_id": file_id_fit,
                   "custom_processing": processing_fit,
                   "custom_parameters": custom_parameters_fit},
            },
            "FIT Quarterly":{
                #Common variables
                "name": config["emis"]["fit"]["name_q"],
                "subdir_substrings":config["emis"]["fit"]["subdir_substrings"],
                "db_dest_table": config["emis"]["fit"]["db_dest_table"],
                "id_cols": config["emis"]["fit"]["data_id_cols"],
                #Functions
                "func":{
                   "file_id": file_id_fit,
                   "custom_processing": processing_fit,
                   "custom_parameters": custom_parameters_fit},
            },
            "SPR":{
                #Common variables
                "name": config["emis"]["spr"]["name"],
                "subdir_substrings":config["emis"]["spr"]["subdir_substrings"],
                "db_dest_table": config["emis"]["spr"]["db_dest_table"],
                "id_cols": config["emis"]["spr"]["data_id_cols"],
                #Dataset variables
                "indicator_name":config["emis"]["spr"]["indicator_name"],
                #Functions
                "func":{
                   "file_id": file_id_spr,
                   "custom_processing": processing_spr,
                   "custom_parameters": custom_parameters_spr},
            }
        }
    }

    return settings