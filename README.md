README

Copyright <2017> <Brysen Keith>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

General Info:
The purpose of this python script is to import machine data stored on the Baxter Prismaflex Dialysis System. The code transforms the data from the Event, Pressure, and Scale semicolon delimited text files into a more usable format and then places the data within a locally housed PostgreSQL database. 

Data Type:
Data is expected to be in the format output by the Baxter Prismaflex software version 5.x in which each therapy prompts the storage of an event, pressures, and scales file. Each file is expected to be in a semicolon delimited .txt file. This script assumes that the prismaflex data is stored in a file entitled 'data'. Location of the file is included in the gitignore and is expected to be added by the user. It is also expected that all .txt files are stored in the 'data' file and not serperated by type. 
Event File: Calibration data is expected to be stored in lines 0 through 19. Event data is expected to be stored after line 21 which contains the column titles 'Index', 'Time',  'ClassCod', 'Class', 'TypeCod', 'Type', 'SampleCod', 'Sample', and 'Fluid'. The script as written accounts for extra space that may be input and also for cells inwhich no data was recorded but data was expected (blank cells are replaced with 'NaN'). The script also pulls out the monitor id and patient id and saves it in each data point line so that it can be later connected to a specific therapeudic run. 
Pressure File: Pressure data is assumed to beign on row 6 with columns titled index, time, access, filter, effl, return, and arps.
Scale File: Scale data is assumed to begin on row 6 with columns titled index, time, runtime, postinf, preinf, dialysate, effluent, prebloodinf, syringeinf, excessfluid, pumpone, pumptwo, pumpthree, and pumpfour.

PostgreSQL Database Storage:
This script was used in conjunction with a PostgreSQL managed database (version 9.6). Psycopg2 is used as the import library allowing for connection between the database and python script. 

HIPAA Compliance:
This script assumes that the only Personal Health Identifier (PHI) included in the data is the manually entered patient id located in the event file. This script utilizes a sha-2 hash function to de-identify this PHI in compliance with CFR 46.101 and HIPAA Safe Harbor Compliance. 

Data Import:
Prismaflex files are imported by determining the file names using a glob function. Each file is then read into Python using the built in csv reader with a ';' delimiter. 


