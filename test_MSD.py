#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 29 22:29:16 2017

@author: ajaver
"""

import sqlite3


db_name = '/Volumes/WormData/Loci_data/Tracking_Results/loci_data_new2.db'
conn = sqlite3.connect(db_name)
cur = conn.cursor()
#%%
import matplotlib.pylab as plt
import numpy as np

def calculate_msd(x,y, msd_shape):
    msds = np.zeros(msd_shape)
    for ind in range(1, msds.size+1):
        delx = x[ind:] - x[:-ind]
        dely = y[ind:] - y[:-ind]
        msds[ind-1] = np.mean(delx*delx + dely*dely)
    return msds

for particle_id in range(1500, 1520): 
    cur.execute('''
    SELECT lag_time, MSD
    FROM particle_MSDs
    WHERE particle_id={}
    '''.format(particle_id))
    result = cur.fetchall()
    if len(result)==0:
        continue
    
    lags, msd = map(np.array, zip(*result))
    
    
    cur.execute('''
    SELECT video_frame, x,y, x_dedrift, y_dedrift
    FROM coordinates
    WHERE particle_id={}
    ORDER BY video_frame
    '''.format(particle_id))
    
    result = cur.fetchall()
    tt, xx, yy, x_dedrift, y_dedrift = map(np.array, zip(*result))
    
    msd_n = calculate_msd(xx,yy, msd.shape)
    msd_d = calculate_msd(x_dedrift,y_dedrift, msd.shape)
        
    plt.figure()
    plt.plot(msd)
    plt.plot(msd_n*(0.106**2 ))
    plt.plot(msd_d*(0.106**2 ))
    print(particle_id)
