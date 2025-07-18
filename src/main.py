#Run time settings
from utils.runtime import *

#Pipeline Scripts
import utils.emis as emis
import utils.pophealth as pop
import utils.screening as scr

settings = build_settings()

if settings["emis"]["run"]:
    emis.emis_dataset_manager(settings, settings["emis"]["zip"])
    print()

if settings["pop"]["run"]:
    print("Running the Population Health Pipeline:")
    pop.indicator_manager(settings)

if settings["screening"]["run"]:
    print("Running the Screening Pipeline")
    scr.process_screening_data(settings, 
                               "./data/screening/national", "national")