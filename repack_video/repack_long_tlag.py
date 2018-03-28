#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 15 22:18:25 2018

@author: ajaver
"""
import os
import cv2
import pandas as pd
import tables
import numpy as np
import tqdm

TABLE_FILTERS = tables.Filters(
        complevel=5,
        complib='zlib',
        shuffle=True,
        fletcher32=True)
        

#%%
import scipy.io
import datetime


#%%
if __name__ == '__main__':
    root_dir = '/run/media/ajaver/Loci I/Nikon/Second_Set/27-June-2012 B18 B21 long/B18_Glu_long/Before'
    #root_dir = '/run/media/ajaver/Loci I/Nikon/Second_Set/28-June-2012 B10 long2s/B10_CAA(4)/Before/'
    
    dnames = os.listdir(root_dir)
    dnames = [x for x in dnames if x != 'Output']
    
    
    all_rows = []
    for dname in tqdm.tqdm(dnames):
        dfullname = os.path.join(root_dir, dname)
        if not os.path.isdir(dfullname):
            continue
        
        fnames = os.listdir(dfullname)
        
        try:
            header_l = [x for x in fnames if not x.endswith('.tif')]
            if header_l != ['header.mat']:
                raise ValueError(header_l)
            header_file = os.path.join(dfullname, header_l[0])
            mat = scipy.io.loadmat(header_file, squeeze_me=False)
            ts_mat = mat['header']
            
            date_str = ts_mat['date'][0][0][0]
            date_t  = datetime.datetime.strptime(date_str, '%d%b%Y')
            
            try:
                h = ts_mat['time'][0][0]['H'][0][0][0][0]
                m = ts_mat['time'][0][0]['M'][0][0][0][0]
                s = ts_mat['time'][0][0]['S'][0][0][0][0]
            except IndexError:
                time_str = ts_mat['time'][0][0][0]
                time_t = datetime.datetime.strptime(time_str, '%H_%M_%S')
                h, m, s = time_t.hour, time_t.minute, time_t.second
                
            ts = datetime.datetime(date_t.year, date_t.month, date_t.day, h, m, s)
            ts_str =  pd.to_datetime(ts).strftime('%Y-%m-%d %H:%M:%S')
            
        except ValueError:
            ts = 'NaN'
        
        
        pos_str, ch_str = dname.split('_')
        
        assert all(x.endswith('.tif') for x in fnames)
        img_list_f = sorted([os.path.join(dfullname, x) for x in fnames if x.endswith('.tif')])
        
        dat = [(*os.path.basename(x)[:-4].split('_'), x) for x in img_list_f]
        dat = [(int(x[0][3:]),  int(x[1][1:]), int(x[2][2:]), x[3]) for x in dat]
        
        
        all_rows += dat
        
        df = pd.DataFrame(all_rows, columns=['position', 'frame_number', 'channel_number', 'file_name'])

    #%%
    for pos_n, dat_pos in tqdm.tqdm(df.groupby('position')):
        save_name = os.path.join(root_dir, 'Pos%03i.hdf5' % pos_n)
        
        with tables.File(save_name, 'w') as fid:
            
            for ch_n, dat_ch in tqdm.tqdm(dat_pos.groupby('channel_number')):
                assert dat_ch['position'].unique().size ==1
                ch_str = 'ch%02i' % ch_n
                
                ini_frame = dat_ch['frame_number'].min()
                n_frames = dat_ch['frame_number'].max() - ini_frame + 1
                image_set = None
                for irow, row in dat_ch.iterrows():
                    img = cv2.imread(row['file_name'], -1)
                    if image_set is None:
                        image_set = fid.create_carray(
                                    '/',
                                    ch_str,
                                    atom = tables.Int16Atom(),
                                    shape=(n_frames, *img.shape),
                                    chunkshape = (1, *img.shape),
                                    filters=TABLE_FILTERS)
                    image_set[row['frame_number']-ini_frame] = img
    

    #%%
    import shutil
    d2remove = set([os.path.dirname(row['file_name']) for irow, row in df.iterrows()])
    for dname in d2remove:
        shutil.rmtree(dname)