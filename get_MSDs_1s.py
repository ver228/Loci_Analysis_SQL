#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 22:15:50 2017

@author: ajaver
"""

import sqlite3
import os

db_name = '/Volumes/WormData/Loci_data/Tracking_Results/loci_data_new.db'
conn = sqlite3.connect(db_name)
cur = conn.cursor()

#%%
sql_experiments = '''
SELECT e.id, e.name, e.date, db.name, s.name, g.name, e.pixel_size, e.delta_time
FROM experiments AS e
JOIN strains AS s ON s.id=e.strain_id
JOIN growth_media AS g ON g.id=e.growth_medium_id
JOIN DB_CSVs AS db ON db.id = e.DB_CSV_id
AND s.loci_location_id<27
AND e.delta_time=1
AND db.name NOT LIKE '%Zhicheng%' 
AND e.perturbation_id=0
WHERE s.knockout_id=0
'''

cur.execute(sql_experiments)
valid_exp = cur.fetchall()

exp_ids, exp_names, exp_dates, db_names, strains, \
media, pixel_sizes, delta_times = zip(*valid_exp)

#%%
exp_groups = {}
for exp_id in exp_ids:
        
    sql_particles = '''
    SELECT p.id
    FROM particles AS p
    JOIN videos AS v ON v.id = p.video_id
    JOIN experiments AS e ON e.id = v.experiment_id
    WHERE e.id = {}
    '''.format(exp_id)
    cur.execute(sql_particles)
    particle_ids = cur.fetchall()
    
    exp_groups[exp_id] = [x for x, in particle_ids]
#%%

def _pix_to_um(data, pix_size):
    return [d*pix_size if d is not None else d for d in data]

def _row_to_str(data):
    dd = ["{:.3f}".format(d) if d is not None else 'NaN' for d in data]
    return '\t'.join(dd)

def _get_p_id_coords(cur, p_id, pix_size):
    sql = '''
    SELECT video_frame, x_dedrift, y_dedrift, signal, background
    FROM coodinates_bckp
    WHERE particle_id = {}
    '''.format(p_id)
    
    cur.execute(sql)
    data = cur.fetchall()
    if len(data) == 0:
        return None
    
    t, x, y, s, b = zip(*data)
    
    x = _pix_to_um(x, pix_size)
    y = _pix_to_um(y, pix_size)
    
    
    return t,x,y,s,b

def _p_coord_str_format(p_id, coord_data):
    str_to_write = 'particle_id {}\n'.format(p_id)
    for name, data in zip(('T', 'X', 'Y', 'S', 'B'), coord_data):
        str_to_write += '{}: {}\n'.format(name, _row_to_str(data))
    return str_to_write
        

save_dir = '/Volumes/WormData/marco_export_1s/'
if not os.path.exists(save_dir):
    os.makedirs(save_dir)


for ii, exp_id in enumerate(exp_ids):
    pix_size = pixel_sizes[ii]
    
    save_name = '{}_{}_{}_1s_ID_{}.csv'.format(strains[ii], media[ii], exp_dates[ii], exp_id)
    print('{} of {}: {} | {}'.format(ii, len(exp_ids),save_name, exp_names[ii]))
    
    full_name = os.path.join(save_dir, save_name)
    with open(full_name, 'wt') as fid:
        for p_id in exp_groups[exp_id]:
            coord_data = _get_p_id_coords(cur, p_id, pix_size)
            if coord_data is not None:
                str_to_write = _p_coord_str_format(p_id, coord_data)        
                fid.write(str_to_write)
    print(save_name, 'DONE')
    


