#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 15 09:14:27 2017

@author: brysenkeith
"""

#import all .txt files from prismaflex data
import glob

list_of_E = glob.glob('*E.TXT')
list_of_P = glob.glob('*P.TXT')
list_of_S = glob.glob('*S.TXT')


import psycopg2
import sys

db = None
try:
    print "Trying to connect to database"
    db = psycopg2.connect("dbname='crrt' user='postgres' host='localhost' host='/tmp/' password='keith1968'")
    print "Connected to database"
    cur = db.cursor()
    print "cursor success"
    cur.execute("DROP TABLE IF EXISTS events;")
    cur.execute("CREATE TABLE events (patient_id text,index text, time text, class_cod text, class text, type_cod text, type text, sample_cod text, sample text, excessfluidlossgain text)")
    cur.execute("DROP TABLE IF EXISTS pressures;")
    cur.execute("CREATE TABLE pressures (index text, time text, access_p text, filter_p text, effluent_p text, return_p text, arps text)")
    
    db.commit()
except psycopg2.DatabaseError, e:
    
    if db:
        db.rollback()
    
    print 'Error %s' % e    
    sys.exit(1)
    
    
finally:
    
    if db:
        db.close()
        
#Load pressures into database        
#need to strip spaces out of columns (strip out white space)
try:
    lines = open(list_of_P[0], 'rb')
    readlines = lines.readlines()
    types = [line.split(";") for line in readlines]
    pressure_data = types[6:]
    indexl = [s[0] for s in pressure_data]
    timel = [s[1] for s in pressure_data]
    accesspl = [s[2] for s in pressure_data]
    filterpl = [s[3] for s in pressure_data]
    efflpl = [s[4] for s in pressure_data]
    returnpl = [s[5] for s in pressure_data]
    arpspl = [s[6] for s in pressure_data]
    db = None
    db = psycopg2.connect("dbname='crrt' user='postgres' host='localhost' host='/tmp/' password='keith1968'")
    cur = db.cursor()        
    cur.execute("INSERT INTO pressures (index, time, access_p, filter_p, effluent_p, return_p, arps) VALUES (%s, %s, %s, %s, %s, %s, %s);", (indexl, timel, accesspl, filterpl, efflpl, returnpl, arpspl))
    db.commit()
except psycopg2.DatabaseError, e:
    
    if db:
        db.rollback()
    
    print 'Error %s' % e    
    sys.exit(1)
    
    
finally:
    if db:
        db.close()

#works for one file, need to iterate over multiple
"""
#for i in range(len(list_of_E)):
lines = open(list_of_E[0], 'rb')
readlines = lines.readlines()
types = [line.split(";") for line in readlines]
calibration_data = types[0:18]
event_data = types[19:]
db = psycopg2.connect("dbname='crrt' user='postgres' host='localhost' host='/tmp/' password='keith1968'")
cur = db.cursor()
cur.execute("INSERT INTO events (index) VALUES (%s);", (event_data[:][0])) 

"""

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
