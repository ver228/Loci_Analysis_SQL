#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 29 22:29:16 2017

@author: ajaver
"""

import sqlite3


db_name = '/Volumes/WormData/Loci_data/Tracking_Results/loci_data.db'
conn = sqlite3.connect(db_name)
cur = conn.cursor()

#%%
sql_experiments = '''
SELECT experiment_id, strain_name, medium_name
FROM experiments AS e 
JOIN strains AS s ON s.strain_id=e.strain_id 
JOIN growth_media AS g ON e.medium_id=g.medium_id
WHERE delta_time=1
AND knockout_id=(SELECT knockout_id FROM knockouts WHERE knockout_name = "NA")
AND perturbation_id = (SELECT perturbation_id FROM perturbations WHERE perturbation_name = "NA")
AND is_fix = 0
AND e.strain_id <=27
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
    SELECT p.particle_id
    FROM particles AS p
    JOIN videos AS v ON v.video_id = p.video_id
    JOIN experiments AS e ON e.experiment_id = v.experiment_id
    WHERE e.experiment_id IN ({})
    '''.format(exp_ids_str)
    cur.execute(sql_particles)
    particle_ids = cur.fetchall()
    
    particles_groups[key] = [x[0] for x in particle_ids]
#%%
import matplotlib.pylab as plt
import numpy as np
particle_id =69818 
#cur.execute('''
#SELECT lag_time, MSD
#FROM particle_MSDs
#WHERE particle_id={}
#'''.format(particle_id))
#result = cur.fetchall()
#lags, msd = map(np.array, zip(*result))
#%%


#%%



#%%
import os
from scipy.io import loadmat

msd_root_dir = '/Volumes/WormData/Loci_data/Tracking_Results/Summary/'

cur.execute('''SELECT e.experiment_id, e.DB_exp_id, d.DB_dir
FROM experiments as e
JOIN DB_names as d ON d.DB_id = e.DB_id;
''')

exp_data = cur.fetchall()

missing_files = []
bad_files = []
for exp_id, DB_exp_id, DB_dir in exp_data:
    file_msd = os.path.join(msd_root_dir, DB_dir, 'Data_MME_%i.mat' % DB_exp_id)
    if not os.path.exists(file_msd):
        missing_files.append(file_msd)
        continue
    
    
    try:
        msd_data = loadmat(file_msd)
        #snr_data = loadmat(file_msd)
    except:
        print('ERROR', file_msd)
        bad_files.append(file_msd)
    
    
    for ii, ids in enumerate(msd_data['trackID'].T):
        cur.execute('''SELECT particle_id 
        FROM particles 
        WHERE video_id={} AND video_particle_id={}'''.format(*ids))
        
        particle_id = cur.fetchone()
        
        
        
        cur.execute('''
        SELECT video_time, coord_x, coord_y
        FROM coordinates
        WHERE particle_id={}
        ORDER BY video_time
        '''.format(particle_id[0]))
        
        result = cur.fetchall()
        tt, xx, yy = map(np.array, zip(*result))
        
        msd = msd_data['timeAv']['MSD'][0,0][:, ii]
        
        msd_n = np.zeros_like(msd)
        for ind in range(1, msd.size+1):
            delx = xx[ind:] - xx[:-ind]
            dely = yy[ind:] - yy[:-ind]
            
            msd_n[ind-1] = np.mean(delx*delx + dely*dely)
    
    
        print(ids, particle_id)
        plt.figure()
        plt.plot(msd)
        plt.plot(msd_n*(0.106**2 ))
        
        break
        
    break