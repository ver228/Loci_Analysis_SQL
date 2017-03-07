# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 14:24:33 2015

@author: ajaver
"""

import sqlite3
import os
import glob
import time
import csv
        

#where the database is going to be created
database_name = '/Users/ajaver/Documents/Loci_data/loci_data.db'

#where the mysql dump files are located
dir_dump_files = '/Users/ajaver/Documents/Loci_data/mysql_dump_separate/';

#script for conversion between mySQL schema to sqlite. Modified from mysql2sqlite.sh (look for it on internet).
mysqlfile2sqlite = './mysqlfile2sqlite.sh';

#If the database existed before, delete it. This code starts from scratch.
if os.path.exists(database_name):
    os.remove(database_name)

conn = sqlite3.connect(database_name);
cur = conn.cursor()

#each of the .sql in the dir_dump_files corresponds to a table schema, while the .txt correspond to the data inside the table.
for sql_file in glob.glob(dir_dump_files + '*.sql'):
    print sql_file
    with open(sql_file) as f:
        #convert the schema using mysqlfile2sqlite script
        sql_commands = os.popen(mysqlfile2sqlite + ' ' + sql_file).read()
        
        #we do not really need to start a transaction, since it is done behind curtains in the sqlite3 python wrapper
        sql_commands= sql_commands.replace('BEGIN TRANSACTION;' , '')
        sql_commands= sql_commands.replace('END TRANSACTION;' , '')
        
        #there is a small bug, where the mysqlfile2sqlite script misses a parenthesis, the code bellow fixes it
        table_name = sql_commands.split('CREATE TABLE `')[1].split('` (')[0]
        
        last_entry_index_first = sql_commands.rfind('\n ', 0)
        last_entry_index_last = sql_commands.find('\n', last_entry_index_first+1)
        if sql_commands[last_entry_index_last-1] == ',':
            cut_left = last_entry_index_last-1;
        else:
            cut_left = last_entry_index_last;
        
        cut_right = last_entry_index_last
        sql_commands_create_table = sql_commands[:cut_left] + '\n);' 
        
        #additionally, script does not deal well with the index declarations. The code bellow fixes
        sql_commands_create_index = sql_commands[cut_right:]
        sql_commands_create_index = sql_commands_create_index.replace(' "" ', ' `'+ table_name +'` ')
        str_ind = -1
        while 1:
            str_ind = sql_commands_create_index.find(' "_" ', str_ind+1)
            if str_ind == -1:
                break;
            
            sql_commands_create_index = sql_commands_create_index[:str_ind] + \
            ' `index_%s_%i` '% (table_name, str_ind) + \
            sql_commands_create_index[str_ind+5:]
            
        #create a table
        cur.executescript(sql_commands_create_table)
        
        #read the csv file and insert the data into the table
        tic = time.time()
        with open(os.path.splitext(sql_file)[0]+'.txt', 'r') as csvfile:
            rowreader = csv.reader(csvfile, delimiter=',', quotechar='"')
            all_data = []
            for row in rowreader: 
                all_data.append(tuple(row))
                if len(all_data) > 10000: #avoid to saturate the memory, since internally the python wrapper made a transaction it does not impact the performance.
                    cur.executemany("INSERT INTO `%s` VALUES(%s)" % (table_name, ','.join(['?']*len(row))), all_data)
                    all_data = []
        
        #create the index after inserting the new values.
        cur.executemany("INSERT INTO `%s` VALUES(%s)" % (table_name, ','.join(['?']*len(row))), all_data)
        
        #print the time it took to insert the data
        print time.time() - tic
        
        # finally create the index
        cur.executescript(sql_commands_create_index)
        conn.commit()

    #create an index for the particle coordinates. I forgot to do that in the original mySQL table
    cur.executescript('CREATE INDEX `index_coordinate_particles` ON `coordinates` (`particle_id`)')
conn.close()
#

