# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 16:47:48 2015

@author: ajaver
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pylab as plt

#where the database is localized.
database_name = '/Users/ajaver/Documents/Loci_data/loci_data.db'

conn = sqlite3.connect(database_name);
cur = conn.cursor()

#Select experiments that use the Left1 strain, LED illumination with a value of 60000 A.U.
#Additionally, select the last set experiments that use IPTG as perturbation, but only when that concentration is zero (this could have been choosen more elegantly using the experiment date).
cur.execute('''
SELECT experiment_id, experiment_name
FROM experiments
WHERE perturbation_id == 
(SELECT perturbation_id FROM perturbations WHERE perturbation_name = 'IPTG')
AND
concentration == 0 
AND illumination_id = (SELECT illumination_id FROM illumination_types WHERE illumination_name = 'LED')
AND illumination_value == 60000
AND strain_id = (SELECT strain_id FROM strains WHERE strain_name = 'Left1');
''')


#select all the particles that where in the previously selected experiments
experiment_list = cur.fetchall()
exp_ids = zip(*experiment_list)[0]
cur.execute('''
SELECT particle_id
FROM particles
WHERE video_id IN (
SELECT video_id
FROM videos
WHERE experiment_id IN (%s))
''' % (','.join([str(x) for x in exp_ids])))
particles_id = zip(*cur.fetchall())[0]

#select particles whose track length is at least 400
cur.execute('''
SELECT particle_id, track_size
FROM (
SELECT particle_id, count(particle_id) as track_size
FROM coordinates
WHERE particle_id IN(%s)
GROUP BY particle_id)
WHERE track_size > 400
''' % (','.join([str(x) for x in particles_id])))
particles_id = zip(*cur.fetchall())[0]

#randomly select 10 particles, plot their intensities vs time and save it as signal_example.pdf
particles_id = np.array(particles_id);
for particle_id in particles_id[np.random.randint(0, len(particles_id), 10)]:
    cur.execute('''
    SELECT particle_id, video_time, coord_signal, coord_background
    FROM coordinates
    WHERE particle_id = %i AND coord_signal != '\N'
    ''' % particle_id)
    signal_data = zip(*cur.fetchall())
    plt.plot(signal_data[1], signal_data[2])
plt.xlabel('Time (frames)')
plt.ylabel('Signal (A.U.)')
plt.savefig('signal_example.pdf')

conn.close()
