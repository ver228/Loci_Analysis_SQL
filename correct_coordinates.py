# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 21:25:08 2015

@author: ajaver
"""
import os
import sqlite3 as sq
import numpy as np
from scipy.io import loadmat

#coord_root_dir = '/Volumes/MyPassport/Data/Tracking_Results/Results'
#database_file = '/Users/ajaver/Desktop/loci_data.db'


coord_root_dir = '/Volumes/WormData/Loci_data/Tracking_Results/Results/'
database_file = '/Volumes/WormData/Loci_data/Tracking_Results/loci_data.db'


bad_files = []

conn = sq.connect(database_file)
cur = conn.cursor()

cur.execute('''
SELECT v.video_id, v.DB_vid_id, d.DB_dir
FROM experiments as e
JOIN videos as v ON e.experiment_id = v.experiment_id
JOIN DB_names as d ON d.DB_id = e.DB_id;
''')
videos_data = cur.fetchall()

for vid_id, DB_vid_id, exp_name in videos_data:
    
    cur.execute('''
    SELECT particle_id, video_particle_id
    FROM particles 
    WHERE video_id = %i;
    ''' % vid_id)
    
    #dictionary to find equivalent between video particle_id and database particle_id 
    dd = cur.fetchall()
    p_dict = {}
    for p,p_vid in dd:
        p_dict[p_vid] = p
    
    file_coord = os.path.join(coord_root_dir, exp_name, 'TrackData_%i.mat' % DB_vid_id)
    assert os.path.exists(file_coord)
    
    file_snr = os.path.join(coord_root_dir, exp_name, 'SNR_%i.mat' % DB_vid_id)
    assert(file_snr)
    
    try:
        coord_data = loadmat(file_coord)
        snr_data = loadmat(file_snr)
    except:
        print('ERROR', file_coord)
        bad_files.append(file_coord)
    
    xx = coord_data['positionsx'].data
    tt = coord_data['positionsy'].indices
    yy = coord_data['positionsy'].data
    
    sig = snr_data['SNRStats']['signal'][0][0].data
    bgnd = snr_data['SNRStats']['bgnd'][0][0].data
    
    pp = np.zeros(tt.shape, np.int)    
    indptr = coord_data['positionsx'].indptr
    for i in range(len(indptr)-1):
        pp[indptr[i]:indptr[i+1]] = i+1
        
    dd = []
    particles_id = []
    for p, t, x, y, s, b in zip(*[pp, tt, xx, yy, sig, bgnd]):
        if p in p_dict.keys():
            p_id = int(p_dict[p])
            dd.append([p_id,int(t),x,y,s,b])
            
            particles_id.append((p_id,))
    
    cur.executemany('''DELETE FROM coordinates 
    WHERE particle_id = ?
    ''', particles_id)
    
    cur.executemany('''INSERT INTO coordinates 
    (particle_id, video_time, coord_x, coord_y, coord_signal, coord_background)
    VALUES (?,?,?,?,?,?)''', dd)
    conn.commit()
    
    #print(dd[0:10])
    
    print(vid_id, len(videos_data), cur.rowcount)

print(bad_files)

#%%
    
    #"UPDATE coordinates SET ?,?,?,?,?,?", dd) 
    #assert np.all(x==x_db)
    #assert np.all(y==y_db)
    
    
#conn.close()