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
    #root_dir = '/Volumes/NTFS/Andrew Data/'
    #root_dir = '/run/media/ajaver/Loci II/Andrew Data' 
    #root_dir = '/run/media/ajaver/Loci II/TnaA Growth' 
    #root_dir = '/run/media/ajaver/Loci II/Nikon/Perturbations/Long Movies 271213' 
    #root_dir = '/run/media/ajaver/Loci II/Nikon/Perturbations/261213 Long Movies' 
    #root_dir = '/run/media/ajaver/Loci I/Nikon/First_Set/11-Oct-2011 B18 Long Movies/ND2/Raw_data/'
    root_dir = '/run/media/ajaver/Loci I/Nikon//First_Set//17-Dec-2011 B(6 25 26 27) CAA+Glu/B6_CAA+Glu/Growth_movies/'
    
    DD = ['/run/media/ajaver/Loci I/Nikon/Second_Set/02-July-2012 Beads/Bad/Beads_ND2/Growth_movies',
 '/run/media/ajaver/Loci I/Nikon/Second_Set/02-July-2012 Beads/Beads_ND128/Growth_movies',
 '/run/media/ajaver/Loci I/Nikon/Second_Set/14-June-2012 S72/S72_CAA_CFP_ND4/Growth_movies',
 '/run/media/ajaver/Loci I/Nikon/Second_Set/14-June-2012 S72/S72_CAA_YFP_ND4/Growth_movies',
 '/run/media/ajaver/Loci I/Nikon/Second_Set/27-June-2012 B18 Indole/B18_CAA_I1/Growth_movies',
 '/run/media/ajaver/Loci I/Nikon/Second_Set/27-June-2012 B18 Indole/B18_CAA_I3/Growth_movies',
 '/run/media/ajaver/Loci I/Nikon/Second_Set/27-June-2012 S72 S134/S134_CAA_CFP/Growth_movies',
 '/run/media/ajaver/Loci I/Nikon/Second_Set/27-June-2012 S72 S134/S72_CAA_CFP(1)/Growth_movies',
 '/run/media/ajaver/Loci I/Nikon/Second_Set/28-June-2012 B10 Indole/B10_CAA/Growth_movies',
 '/run/media/ajaver/Loci I/Nikon/Second_Set/28-June-2012 B10 Indole/B10_EtOH/Growth_movies',
 '/run/media/ajaver/Loci I/Nikon/Second_Set/28-June-2012 B10 Indole/B10_I1/Growth_movies',
 '/run/media/ajaver/Loci I/Nikon/Second_Set/28-June-2012 B10 Indole/B10_I2/Growth_movies',
 '/run/media/ajaver/Loci I/Nikon/Second_Set/28-June-2012 B10 Indole/B10_I3/Growth_movies']
    
    
    for root_dir in DD:
        dnames = os.listdir(root_dir)
        dnames = [x for x in dnames if x != 'Output']
        dnames = [x for x in dnames if x != 'PhC']
        
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
            
            
            pos_str, tt_str, ch_str = dname.split('_')
            img_list_f = sorted([os.path.join(dfullname, x) for x in fnames if x.endswith('.tif')])
            
            
            image_n = [int(os.path.splitext(os.path.basename(x))[0]) for x in img_list_f]
            
            pos_n = int(pos_str[3:])
            tt = int(tt_str[1:])
            ch_n = int(tt_str[2:])
            
            dd = [(pos_n, tt, ch_n, ts_str, *x) for x in zip(image_n, img_list_f)]
            
            all_rows += dd
            
            df = pd.DataFrame(all_rows, columns=['position', 'frame_number', 'channel_number', 'timestamp', 'image_number', 'file_name'])
        
        #%%
        df = df.sort_values(by=['position', 'frame_number', 'channel_number', 'image_number'])
        
        save_name = os.path.join(root_dir, 'RawImages.hdf5')
        image_set = None
        with tables.File(save_name, 'w') as fid:
            df_s = df[['position', 'frame_number', 'channel_number', 'timestamp', 'image_number']].copy()
            
            tab_recarray = df_s.to_records(index=False)
            tab_recarray = tab_recarray.astype(np.dtype([
                    ('position', '<i4'), 
                    ('channel_number', '<i4'), 
                    ('frame_number', '<i4'), 
                    ('timestamp', '<S19'),
                    ('image_number', '<i4')
                                      ]))
            info_d = fid.create_table(
                '/',
                'info',
                obj = tab_recarray,
                filters = TABLE_FILTERS)
            
            for irow, row in tqdm.tqdm(df.iterrows(), total=len(df)):
                img = cv2.imread(row['file_name'], -1)
                if img is None:
                    continue
                if image_set is None:
                    image_set = fid.create_carray(
                                '/',
                                'data',
                                atom = tables.Int16Atom(),
                                shape=(len(df), *img.shape),
                                chunkshape = (1, *img.shape),
                                filters=TABLE_FILTERS)
                
                image_set[irow] = img
    
        #%%
        import shutil
        d2remove = set([os.path.dirname(row['file_name']) for irow, row in df.iterrows()])
        for dname in d2remove:
            if os.path.exists(dname):
                shutil.rmtree(dname)