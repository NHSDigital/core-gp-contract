# core-gp-contract
Repository for code related to the Core GP Contract GPES extract

# ALCOHOL AND GMS/PMS PUBLICATION JOB
This job outputs four quarterly CSVs for the entire financial year after suppression applied 

## How to run
The main file to run is 'main.ipynb'. The code contains databrick widgets to display report times.

## Installations
The required python packages to run this publication job are:  pyspark, pandas, unittests, typing & functools.

## Parameters
The file "parameters.ipynb" contains date parameters which can be changed to fit the dates required for your personalised report. They are also various SQL database inputs required to put in in pre-processing and checks.

## Functions folder
The functions folder contains various functions used in the job which have been conveniently split into different sections. The "run_functions.ipynb" runs all the files in the functions in the functions file. This is done for ease of access in the "main.ipynb" file.

## Tests folder
The tests folder contains various normal tests and unittests for each function of the code to check whether they are functioning correctly.

## Spot checking
As the code is throughly checked and tested, the spot checking part of the code is done to manually assess the output for any errors the job did not catch.
 
## Contact details
Contact Details		
Author: Primary Care Domain, NHS England		
Lead Analyst Rob Danks		
Public Enquiries: Telephone: 0300 303 5678		
Email: enquiries@nhsdigital.nhs.uk		
Press enquiries should be made to: Media Relations Manager: Telephone: 0300 303 3888		

## Copyright
Published by NHS England, part of the Government Statistical Service		
Copyright © 2023, NHS England.				
You may re-use this document/publication (not including logos) free of charge in any format or medium, under the  terms of the		
Open Government Licence v3.0		
or write to the Information Policy Team, The National Archives,		
Kew, Richmond, Surrey, TW9 4DU;		
or email: psi@nationalarchives.gsi.gov.uk		