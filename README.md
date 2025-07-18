# Cancer Primary Care Dashboard ETL Process

ETL code for processing data to feed the NCL CA Cancer Primary Care Dashboard. 
This includes:
 - Data extracted from EMIS
   - Cancer Care Reviews
   - eSafety Netting
   - FIT
   - Social Prescribing Referrals
 - Population Health Data from Fingertips

## Changelog

### [1.0.0] - 2025-04-24
#### Added
- Core functionality for EMIS and Population Health

#### [1.0.1] - 2025-05-13 
- Fixed an issue with decimal points and truncation
- Fixed an issue with the derived date being incorrect

#### [1.0.2] - 2025-07-03
- Fixed an issue with the derived date not transitioning between years correctly
- Added an evironment variable to disable the upload portion of the code for testing
- Added documentation for the code
- Added a sample.env file to help people build the .env for themselves

#### [1.1.0] - 2025-07-16
- Added ability to process quarterly fit data

#### [1.2.0] - 2025-07-18
- Added Screening pipeline

## Set up
Please refer to the NCL ICB Analytics Team Scripting Onboarding documentation for instructions on setting up coding projects including virtual environments (venv).

The Scripting Onboarding documentation is available here: [Scripting Onboarding](https://nhs.sharepoint.com/:f:/r/sites/msteams_3c6e53/Shared%20Documents/Data%20Science?csf=1&web=1&e=ArWnMM)

If the .env file is not already in the top level of the project directory you can either:
- Follow the instructions in the sample.env file to create one
- Copy into the top level of this directory, the existing .env file from the Cancer Primary Care Dashboard working directory Documents folder on the shared drive. 

This project requires the following files to be saved in corresponding locations:

If EMIS_ZIPFILE is set to "True" in the .env file:
- EMIS zip file: Zip file containing EMIS data
  - Saved in ./data/emis/

If EMIS_ZIPFILE is set to "False" in the .env file:
- CCR directory containing the CCR data files
  - Saved in ./data/Cancer/
- eSafety Netting directory containing the CCR data files
  - Saved in ./data/safety netting/
- FIT directory containing the FIT data files
  - Saved in ./data/FIT/
- Social Prescribing Referral directory containing the SPR data files
  - Saved in ./data/Social prescribing referral/

To use Screening data, the csvs from the Futures Screening dashboard should be added to the data/screening/ folder in either the local (for NCL practice level data) or national (for regional benchmarking data)

## Usage
- Complete the set up as described in the previous section.
- Enable the virtual environment if not already active (see [Scripting Onboarding](https://nhs.sharepoint.com/:w:/r/sites/msteams_38dd8f/Shared%20Documents/Document%20Library/Documents/Git%20Integration/Internal%20Scripting%20Guide.docx?d=wc124f806fcd8401b8d8e051ce9daab87&csf=1&web=1&e=OQjbRm) documentation for details).
- Adjust the settings in the .env to toggle which files and data is processed
- Run the main script ./src/main.py

##
*The contents and structure of this template were largely based on the template used by the NCL ICB Analytics team available here: [NCL ICB Project Template](https://github.com/ncl-icb-analytics/ncl_project)*

## Licence
This repository is dual licensed under the [Open Government v3]([https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) & MIT. All code can outputs are subject to Crown Copyright.

## Contact
Jake Kealey - jake.kealey@nhs.net

Project Link: https://github.com/ncl-cancer-alliance/cancer_primary_care_etl