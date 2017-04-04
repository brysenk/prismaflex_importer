#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 15 09:14:27 2017

@author: brysenkeith
"""
#%%
#import all .txt files from prismaflex data
import glob
import datetime
import csv
import pandas
from datetime import datetime


list_of_E = glob.glob('/Users/brysenkeith/workspace/prismaflex_data/*/*E.TXT')
list_of_P = glob.glob('/Users/brysenkeith/workspace/prismaflex_data/*/*P.TXT')
list_of_S = glob.glob('/Users/brysenkeith/workspace/prismaflex_data/*/*S.TXT')


import psycopg2
import sys

db = None
try:
    print "Trying to connect to database"
    db = psycopg2.connect("dbname='crrt' user='brysenkeith' host='localhost' host='/tmp/'")
    print "Connected to database"
    cur = db.cursor()
    print "cursor success"
    cur.execute("DROP TABLE IF EXISTS events;")
    cur.execute("CREATE TABLE events (index INT, time timestamp, class_cod INT, class text, type_cod INT, type text, sample_cod INT, sample text, excessfluidlossgain text)")
    
    cur.execute("DROP TABLE IF EXISTS pressures;")
    cur.execute("CREATE TABLE pressures (index INT, time timestamp, access_p INT, filter_p INT, effluent_p INT, return_p INT, arps INT)")
    
    cur.execute("DROP TABLE IF EXISTS scales;")
    cur.execute("CREATE TABLE scales (indexS INT, timeS timestamp, runtimeS INT, postinfS INT, preinfS INT, dialysateS INT, effluentS INT, prebloodinfS INT, syringeinfS INT, excessfluidS INT, pumpone INT, pumptwo INT, pumpthree INT, pumpfour INT)")
    
    db.commit()
except psycopg2.DatabaseError, e:
    
    if db:
        db.rollback()
    
    print 'Error %s' % e    
    sys.exit(1)
    
    
finally:
    
    if db:
        db.close()
 #%%      
#Load pressures into database        
#need to strip spaces out of columns (strip out white space)

try:
    db = None
    db = psycopg2.connect("dbname='crrt' user='brysenkeith' host='localhost' host='/tmp/'")
    cur = db.cursor()
    for p file in list of p #set run id so that it iterates
    lines = open(list_of_P[0], 'rb').readlines()

    pressure_data = csv.reader(lines, delimiter=';')
    for index, row in enumerate(pressure_data):
        if index < 6:
            continue
        else:
            indexpl = int(row[0])
            #timel = str(row[1]) #this needs to be a timestamp look at strftime and strptime
            timel = datetime.strptime(row[1], '%c') #check to make sure format is correct
            accesspl = int(row[2])
            filterpl = int(row[3])
            efflpl = int(row[4])
            returnpl = int(row[5])
            arpspl = int(row[6])
           
            cur.execute('INSERT INTO pressures (index, time, access_p, filter_p, effluent_p, return_p, arps) VALUES (%s, %s, %s, %s, %s, %s, %s)', (indexpl, timel, accesspl, filterpl, efflpl, returnpl, arpspl))
    
    db.commit()
    
    
    
except psycopg2.DatabaseError, e:
    
    if db:
        db.rollback()
    
    print 'Error %s' % e    
    sys.exit(1)
    
    
finally:
    if db:
        db.close()

    #%%

    

 #%%
#works for one file, need to iterate over multiple
#Load Events into Database
import codecs
try:
    lines = open(list_of_E[2], 'rb')
    readlines = lines.readlines()
    types = [line.split(";") for line in readlines]
    calibration_data = types[:18]
    
    db = None
    db = psycopg2.connect("dbname='crrt' user='brysenkeith' host='localhost' host='/tmp/'")
    cur = db.cursor()
    
    f=codecs.open(list_of_E[2],"rb","utf-16")
    event_data = csv.reader(f,delimiter=';')
    index = 0
    for index, row in enumerate(event_data):
        if index == 0:
            monitor = row
            monitor_id = monitor[0][11:]
        if index < 20:
            continue
        else:
            indexE = int(row[0])
            #timel = str(row[1]) #this needs to be a timestamp look at strftime and strptime
            timeE = datetime.strptime(row[1], '%c') #check to make sure format is correct
            classcodE = int(row[2])
            classE = row[3]
            typecodE = int(row[4])
            typeE = row[5]
            samplecodE = int(row[6])
            sampleE = row[7]
            if index == 21:
                patient_id = sampleE
            if not sampleE:
                sampleE = 'NaN'
            fluidE = row[8]
            if not fluidE:
                fluidE = 'NaN'
           
            cur.execute('INSERT INTO events (index, time, class_cod, class, type_cod, type, sample_cod, sample, excessfluidlossgain) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', (indexE, timeE, classcodE, classE, typecodE, typeE,samplecodE, sampleE, fluidE))
    
    db.commit()
    
    print 'Event data upload succesful'
    
    
except psycopg2.DatabaseError, e:
    
    if db:
        db.rollback()
    
    print 'Error %s' % e    
    sys.exit(1)
    
    
finally:
    if db:
        db.close()
"""
#for i in range(len(list_of_E)):
lines = open(list_of_E[0], 'rb')
readlines = lines.readlines()
types = [line.split(";") for line in readlines]
calibration_data = types[0:18]
event_data = types[20:]
indexE = [s[0] for s in event_data]
timeE = [s[1] for s in event_data]
classcodE = [s[2] for s in event_data]
classE = [s[3] for s in event_data]
typecodE = [s[4] for s in event_data]
typeE = [s[5] for s in event_data]
samplecodE = [s[6] for s in event_data]
sampleE = [s[7] for s in event_data]
fluidE = [s[8] for s in event_data]


db = psycopg2.connect("dbname='crrt' user='brysenkeith' host='localhost' host='/tmp/'")
cur = db.cursor()
cur.execute("INSERT INTO events (index) VALUES (%s);", (event_data)) 
"""

#%%
#Import scale files (S.txt) into PSQL database

try:
    db = None
    db = psycopg2.connect("dbname='crrt' user='brysenkeith' host='localhost' host='/tmp/'")
    cur = db.cursor()
    
    lines = open(list_of_S[0], 'rb').readlines()
    scale_data = csv.reader(lines, delimiter=';')
    index = 0
    for index, row in enumerate(scale_data):
        if index < 6:
            continue
        else:
            indexS = int(row[0])
            #timel = str(row[1]) #this needs to be a timestamp look at strftime and strptime
            timeS = datetime.strptime(row[1], '%c') #check to make sure format is correct
            runtimeS = int(row[2])
            postinfS = int(row[3])
            preinfS = int(row[4])
            dialysateS = int(row[5])
            effluentS = int(row[6])
            prebloodinfS = int(row[7])
            syringeinfS = int(row[8])
            excessfluidS = int(row[9])
            pumpone = int(row[10])
            pumptwo = int(row[11])
            pumpthree = int(row[12])
            pumpfour = int(row[13])
           
            cur.execute('INSERT INTO scales (indexS, timeS, runtimeS, postinfS, preinfS, dialysateS, effluentS, prebloodinfS, syringeinfS, excessfluidS, pumpone, pumptwo, pumpthree, pumpfour) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (indexS, timeS, runtimeS, postinfS, preinfS, dialysateS, effluentS, prebloodinfS, syringeinfS, excessfluidS, pumpone, pumptwo, pumpthree, pumpfour))
    
    db.commit()
    print 'Scale Upload Succesful'
    
    
    
except psycopg2.DatabaseError, e:
    
    if db:
        db.rollback()
    
    print 'Error %s' % e    
    sys.exit(1)
    
    
finally:
    if db:
        db.close()

#%%
        
        
"""
psuedocode for python sql import
GOAL: have 4 tables of the P,S,E,C-stripped off of E format
Simple python script to run all of these


files = Read files
files.each do |file|
    var e_file = file for e
    var file_code = strip of leading number off of file for e
    var s_file = find s_file ex. find_file(type, code)
    var p_file = find p_file
    
    split files (ex E file needs to be split at line 19, create a c_file)
    
    import_e_file = function that imports file into postgres
    on the import add a column that has the leading_number on it plus some unique
    identifier in case later down the road they have duplicates
    ...do this for the other files
"""
