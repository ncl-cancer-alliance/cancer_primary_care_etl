#Run time settings
from utils.runtime import *

#Pipeline Scripts
import utils.emis as emis
import utils.pophealth as pop

settings = build_settings()

if settings["emis"]["run"]:
    emis.emis_dataset_manager(settings, settings["emis"]["zip"])

if settings["pop"]["run"]:
    print("Running the Population Health Pipeline:")
    pop.indicator_manager(settings)