# This file contains the code to manage runtime settings as well as build the 
# settings dictionary object

import toml
from dotenv import load_dotenv

def build_settings():
    #Load env settings
    load_dotenv(override=True)

    #Load toml settings from config
    config = toml.load("./config.toml")

    settings = {
        "base_dir": config["base"]["data_dir"]
    }