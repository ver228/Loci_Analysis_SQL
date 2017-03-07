#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 14:48:52 2017

@author: ajaver
"""

import sqlite3
import os

#db_name = '/Volumes/WormData/Loci_data/Tracking_Results/loci_data_new.db'
db_name = '/Volumes/behavgenom_archive$/Avelino/Others/loci_data_new.db'
conn = sqlite3.connect(db_name)
cur = conn.cursor()

sql_experiments = '''
SELECT e.id AS experiment_id, 
e.name AS experiment_name, 
s.name AS strain_name,
g.name AS growth_media,
e.pixel_size AS `pixel_size(um)`, 
e.delta_time AS `delta_time(s)`,
pt.details AS perturbation, 
p.concentration AS perturbation_concentration, 
it.name AS illumination_type, 
i.value AS illumination_value,
e.date AS experiment_date, 
db.name AS set_name
FROM experiments AS e
JOIN strains AS s ON s.id=e.strain_id
JOIN growth_media AS g ON g.id=e.growth_medium_id
JOIN DB_CSVs AS db ON db.id = e.DB_CSV_id
JOIN perturbations AS p ON p.id = e.perturbation_id
JOIN perturbation_types AS pt ON pt.id = p.perturbation_type_id
JOIN illuminations AS i ON i.id = e.illumination_id
JOIN illumination_types AS it ON it.id = i.illumination_type_id;
'''

cur.execute(sql_experiments)
valid_exp = cur.fetchall()

for row in valid_exp:
	print(row)
