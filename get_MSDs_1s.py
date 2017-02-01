#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 22:15:50 2017

@author: ajaver
"""

import sqlite3


db_name = '/Volumes/WormData/Loci_data/Tracking_Results/loci_data_new.db'
conn = sqlite3.connect(db_name)
cur = conn.cursor()

#%%
sql_experiments = '''
SELECT e.id, s.name, g.name
FROM experiments AS e
JOIN strains AS s ON s.id=e.strain_id
JOIN growth_media AS g ON g.id=e.growth_medium_id
WHERE s.knockout_id=0
AND s.loci_location_id<27
AND e.perturbation_id=0
AND e.delta_time=1
'''

cur.execute(sql_experiments)
valid_exp = cur.fetchall()

#%%
exp_groups = {}
for exp_id, strain, medium in valid_exp:
    key = (strain, medium)
    if not key in exp_groups:
        exp_groups[key] = []
    exp_groups[key].append(exp_id)

particles_groups = {}
for key, exp_ids in exp_groups.items():
    exp_ids_str = ','.join(map(str, exp_ids))
        
    sql_particles = '''
    SELECT p.id
    FROM particles AS p
    JOIN videos AS v ON v.id = p.video_id
    JOIN experiments AS e ON e.id = v.experiment_id
    WHERE e.id IN ({})
    '''.format(exp_ids_str)
    cur.execute(sql_particles)
    particle_ids = cur.fetchall()
    
    particles_groups[key] = [x[0] for x in particle_ids]