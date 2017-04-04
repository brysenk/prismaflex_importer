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
    cur.execute("CREATE TABLE events (patient_id text, monitor_id text, index INT, time timestamp, class_cod INT, class text, type_cod INT, type text, sample_cod INT, sample text, excessfluidlossgain text)")
    
    cur.execute("DROP TABLE IF EXISTS pressures;")
    cur.execute("CREATE TABLE pressures (patient_id text, monitor_id text, index INT, time timestamp, access_p INT, filter_p INT, effluent_p INT, return_p INT, arps INT)")
    
    cur.execute("DROP TABLE IF EXISTS scales;")
    cur.execute("CREATE TABLE scales (patient_id text, monitor_id text, indexS INT, timeS timestamp, runtimeS INT, postinfS INT, preinfS INT, dialysateS INT, effluentS INT, prebloodinfS INT, syringeinfS INT, excessfluidS INT, pumpone INT, pumptwo INT, pumpthree INT, pumpfour INT)")
    
    db.commit()
except psycopg2.DatabaseError, e:
    
    if db:
        db.rollback()
    
    print 'Error %s' % e    
    sys.exit(1)
    
    
finally:
    
    if db:
        db.close()


#Load Events into Database
import codecs
try:
    #lines = open(list_of_E[2], 'rb')
    #readlines = lines.readlines()
    #types = [line.split(";") for line in readlines]
    #calibration_data = types[:18]
    
    db = None
    db = psycopg2.connect("dbname='crrt' user='brysenkeith' host='localhost' host='/tmp/'")
    cur = db.cursor()
    
    counter = 0
    for counter, row in enumerate(list_of_E):
    
        f=codecs.open(list_of_E[counter],"rb","utf-16")
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
               
                cur.execute('INSERT INTO events (patient_id, monitor_id, index, time, class_cod, class, type_cod, type, sample_cod, sample, excessfluidlossgain) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (patient_id, monitor_id, indexE, timeE, classcodE, classE, typecodE, typeE,samplecodE, sampleE, fluidE))
        
                db.commit()
        print "Event data upload for ", list_of_E[counter], " succesful"
        #pressures
        lines = open(list_of_P[counter], 'rb').readlines()
    
        pressure_data = csv.reader(lines, delimiter=';')
        index = 0
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
               
                cur.execute('INSERT INTO pressures (patient_id, monitor_id, index, time, access_p, filter_p, effluent_p, return_p, arps) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', (patient_id, monitor_id, indexpl, timel, accesspl, filterpl, efflpl, returnpl, arpspl))
        
            db.commit()
        print "Pressure data upload for ", list_of_P[counter], " succesful"
        
        lines = open(list_of_S[counter], 'rb').readlines()
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
               
                cur.execute('INSERT INTO scales (patient_id, monitor_id, indexS, timeS, runtimeS, postinfS, preinfS, dialysateS, effluentS, prebloodinfS, syringeinfS, excessfluidS, pumpone, pumptwo, pumpthree, pumpfour) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (patient_id, monitor_id, indexS, timeS, runtimeS, postinfS, preinfS, dialysateS, effluentS, prebloodinfS, syringeinfS, excessfluidS, pumpone, pumptwo, pumpthree, pumpfour))
        
                db.commit()
        print "Scale data upload for ", list_of_S[counter], " succesful"   
 
  
               
    print 'Data upload succesful'
    
    
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
    
    counter = 0
    for counter, row in enumerate(list_of_P):
    
        lines = open(list_of_P[counter], 'rb').readlines()
    
        pressure_data = csv.reader(lines, delimiter=';')
        index = 0
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
               
                cur.execute('INSERT INTO pressures (patient_id, monitor_id, index, time, access_p, filter_p, effluent_p, return_p, arps) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', (patient_id, monitor_id, indexpl, timel, accesspl, filterpl, efflpl, returnpl, arpspl))
        
            db.commit()
        print "Pressure data upload for ", list_of_P[counter], " succesful"
    print 'Pressure upload succesful'
    
    
except psycopg2.DatabaseError, e:
    
    if db:
        db.rollback()
    
    print 'Error %s' % e    
    sys.exit(1)
    
    
finally:
    if db:
        db.close()


#%%
#Import scale files (S.txt) into PSQL database
try:
    db = None
    db = psycopg2.connect("dbname='crrt' user='brysenkeith' host='localhost' host='/tmp/'")
    cur = db.cursor()
    
    counter = 0
    for counter, row in enumerate(list_of_S):
    
        lines = open(list_of_S[counter], 'rb').readlines()
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
               
                cur.execute('INSERT INTO scales (patient_id, monitor_id, indexS, timeS, runtimeS, postinfS, preinfS, dialysateS, effluentS, prebloodinfS, syringeinfS, excessfluidS, pumpone, pumptwo, pumpthree, pumpfour) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (patient_id, monitor_id, indexS, timeS, runtimeS, postinfS, preinfS, dialysateS, effluentS, prebloodinfS, syringeinfS, excessfluidS, pumpone, pumptwo, pumpthree, pumpfour))
        
                db.commit()
        print "Scale data upload for ", list_of_S[counter], " succesful"
        
    print 'Scale Upload Succesful'
    
    
    
except psycopg2.DatabaseError, e:
    
    if db:
        db.rollback()
    
    print 'Error %s' % e    
    sys.exit(1)
    
    
finally:
    if db:
        db.close()

