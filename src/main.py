#Run time settings
from utils.runtime import *

#Pipeline Scripts
from utils.emis import *

settings = build_settings()

if settings["emis"]["run"]:
    emis_dataset_manager(settings, settings["emis"]["zip"])