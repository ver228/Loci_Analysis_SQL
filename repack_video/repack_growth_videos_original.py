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
import shutil
import tqdm


TABLE_FILTERS = tables.Filters(
        complevel=5,
        complib='zlib',
        shuffle=True,
        fletcher32=True)




#%%
import scipy.io
import datetime

def _read_header(fname):
    try:
        mat = scipy.io.loadmat(fname, squeeze_me=False)
    except:
        return None
    
    timedata_l = []
    
    timeData = mat['timeData']
    
    
    tot_channels, tot_timestamps = timeData.shape
    for nch in range(tot_channels):
        for tt in range(tot_timestamps):
            ts_mat = timeData[nch, tt]
            
            if ts_mat.size > 0:
            
                date_str = ts_mat['date'][0][0][0]
                date_t  =datetime.datetime.strptime(date_str, '%d%b%Y')
                try:
                    h = ts_mat['time'][0][0]['H'][0][0][0][0]
                    m = ts_mat['time'][0][0]['M'][0][0][0][0]
                    s = ts_mat['time'][0][0]['S'][0][0][0][0]
                
                    
                except IndexError:
                    time_str = ts_mat['time'][0][0][0]
                    time_t = datetime.datetime.strptime(time_str, '%H_%M_%S')
                    h, m, s = time_t.hour, time_t.minute, time_t.second
                timedata_l.append((nch, tt, datetime.datetime(date_t.year, date_t.month, date_t.day, h, m, s)))
    
    header_df = pd.DataFrame(timedata_l, columns=['channel_number', 'frame_number', 'timestamp'])
    header_df['timestamp'] = pd.to_datetime(header_df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return header_df

        
def _process_directory(dname):
    if not os.path.isdir(dname):
        return
    
    fnames = os.listdir(dname)
    headers = [x for x in fnames if not x.endswith('.tif')]
    if headers != ['timeData.mat']:
        raise ValueError(headers)
    
    header_file = os.path.join(dname, headers[0])
    header_info = _read_header(header_file)
    
    img_file_list = [x for x in fnames if x.endswith('.tif')]
    
    if len(img_file_list) > len(header_info):
        print(len(img_file_list), len(header_info))
        import pdb
        pdb.set_trace()
        raise ValueError(len(img_file_list), len(header_info))
    
    tot_frames = header_info['frame_number'].max() - header_info['frame_number'].min() + 1
    tot_channels = header_info['channel_number'].unique().size
    
    bad_data = []
    img_data = {}
    for fname in tqdm.tqdm(img_file_list, desc = dname):
        img_f = os.path.join(dname, fname)
        img = cv2.imread(img_f, -1)
        
        pos_N, tt_str, ch_str = fname[:-4].split('_')
        
        if not ch_str in img_data:
            img_data[ch_str] = np.zeros((tot_frames, *img.shape), dtype=img.dtype)
        
        ii = int(tt_str[1:]) -1
        
        if img is None:
            bad_data.append(img_f)
        else:
            img_data[ch_str][ii] = img
        
    if bad_data:
        print('BAD!!!', bad_data)
        return
        
    if len(img_data) != tot_channels:
        print(img_data.keys())
        import pdb
        pdb.set_trace()
        raise ValueError(img_data.keys())
    
    pos_str_l = [x.partition('_')[0] for x in img_file_list]
    pos_str = pos_str_l[0]
    assert all(pos_str == x for x in pos_str_l)
    
    
    save_name = os.path.join(os.path.dirname(dname), pos_str + '.hdf5')
    with tables.File(save_name, 'w') as fid:
        
        for ch_str, images in img_data.items():
            fid.create_carray(
                            '/',
                            ch_str,
                            obj=images,
                            chunkshape = (1, *images.shape[1:]),
                            filters=TABLE_FILTERS)
        
        
        
        tab_recarray = header_info.to_records(index=False)
        tab_recarray = tab_recarray.astype(np.dtype([
                                    ('channel_number', '<i4'), 
                                  ('frame_number', '<i4'), 
                                  ('timestamp', '<S19')
                                  ]))
        info_d = fid.create_table(
            '/',
            'info',
            obj = tab_recarray,
            filters = TABLE_FILTERS)
        
        info_d._v_attrs['has_finished'] = 1
        
    #remove processed directories
    shutil.rmtree(dname)
    return
    
        
if __name__ == '__main__':
    #root_dir = '/Volumes/NTFS/Andrew Data/'
    #root_dir = '/run/media/ajaver/Loci II/Andrew Data' 
    #root_dir = '/run/media/ajaver/Loci II/TnaA Growth' 
    #root_dir = '/run/media/ajaver/Loci II/Nikon/Perturbations/Long Movies 271213' 
    #root_dir = '/run/media/ajaver/Loci I/Nikon/First_Set/11-Oct-2011 B18 Long Movies/ND2/Output/'
    #root_dir = '/run/media/ajaver/Loci I/Nikon/First_Set/14-Sep-2011 DnaWT CAA+Glu Andor-low/Output'
    root_dir = '/run/media/ajaver/Loci I/Nikon/Forth_Set//Growth Data FM4-64/200113 CAA_B9_FM 30C/'
    
    dnames = os.listdir(root_dir)
    dnames = [x for x in dnames if x != 'Results']
    dnames = [x for x in dnames if x != 'Raw']
    bad_files = []
    for dname in dnames[::-1]:
        full_name = os.path.join(root_dir, dname)
        
        if not os.path.isdir(full_name):
            continue
        
        
        while True:
            try:
                _process_directory(full_name)
            except ValueError:
                raise
                bad_files.append(dname)
                print('?')
                #continue
            break
        
    print(bad_files)