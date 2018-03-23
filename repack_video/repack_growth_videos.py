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

ZERO_CORRUPTED = False

TABLE_FILTERS = tables.Filters(
        complevel=5,
        complib='zlib',
        shuffle=True,
        fletcher32=True)

def _read_img_list(dname, img_file_list, desc=''):
    #I am lazy here. I will read all the images first and then I save them
    images = []
    assert all(x.endswith('.tif') for x in img_file_list)
    img_file_list = sorted(img_file_list, key = lambda x : int(x[:-4]))
    
    pbar = tqdm.tqdm(img_file_list, desc=desc)
    for fname in pbar:
        img = cv2.imread(os.path.join(dname, fname), -1)
        images.append(img)
    
    if ZERO_CORRUPTED:
        img_shape = None
        for img in images:
            if img is not None:
                img_shape = img.shape
                img_dtype = img.dtype
            break
    
        if img_shape is None:
            return np.array([])
    
        #remove images that could not be read
        images = [x if x is not None else np.zeros(img_shape, img_dtype) for x in images]
    
   
    images = np.array(images)
    return images

def _process_pos(main_dir, pos, dnames):
    save_name = os.path.join(main_dir, pos + '.hdf5')
    if os.path.exists(save_name):
        try:
            with tables.File(save_name, 'r') as fid:
                if tables.get_node('/info')._v_attrs['has_finished'] == 1:
                    return
        except:
            pass
    
    with tables.File(save_name, 'w') as fid:
        pos_tab = []
        for dname in dnames:
            fnames = os.listdir(dname)
            ch_str = os.path.basename(dname).partition('_')[-1].lower()
            
            img_files = [x for x in fnames if x.endswith('.tif')]
            csv_files = [x for x in fnames if x.endswith('.csv')]
            
            if len(set(fnames)-set(img_files+csv_files)) != 0 or \
                any(not x in ('illumVec.csv', 'timeVec.csv') for x in csv_files):
                
                raise ValueError('There are extra files that i do not recognize.')
            
            
            
            illum_file = os.path.join(dname, 'illumVec.csv')
            if os.path.exists(illum_file):
                #read the extra information
                df = pd.read_csv(illum_file, 
                                        header=None, 
                                        usecols = [0,1,2],
                                        names = ['frame', 'photodiode_val', 'led_current']
                                        )
                
            else:
                df = pd.DataFrame()
            
            time_vec = pd.read_csv(os.path.join(dname, 'timeVec.csv'), 
                                       header=None)
            
            time_vec = time_vec.dropna(how='all', axis=1)
            if time_vec.shape[1] == 7:
                time_vec = time_vec.iloc[:, 1:]
            
            assert time_vec.shape[1] == 6
            
            time_vec.columns = ['year','month','day', 'hour', 'minute', 'second']
            
            df['timestamp'] = pd.to_datetime(time_vec).dt.strftime('%Y-%m-%d %H:%M:%S')
            df['channel'] = ch_str
            
            pos_tab.append(df)
            
            images = _read_img_list(dname, img_files, desc= '{}_{}'.format(pos, ch_str))
            
            if any(x is None for x in images):
                raise ValueError
            
            fid.create_carray(
                            '/',
                            ch_str,
                            obj=images,
                            chunkshape = (1, *images.shape[1:]),
                            filters=TABLE_FILTERS)
        
        #save the extra information
        pos_tab = pd.concat(pos_tab)
        
        
        
        tab_recarray = pos_tab.to_records(index=False)
        tab_recarray = tab_recarray.astype(np.dtype([('frame', '<i4'), 
                                  ('photodiode_val', '<f4'), 
                                  ('led_current', '<f4'), 
                                  ('timestamp', '<S19'), 
                                  ('channel', '<S4')]))
        info_d = fid.create_table(
            '/',
            'info',
            obj = tab_recarray,
            filters = TABLE_FILTERS)
        
        info_d._v_attrs['has_finished'] = 1
    
    #remove processed directories
    for dname in dnames:
            shutil.rmtree(dname)

def _process_directory(main_dir):
    positions_d = {}
    for bn in os.listdir(main_dir):
        dname = os.path.join(main_dir, bn)
        if not os.path.isdir(dname):
            continue
        
        if 'Raw' in dname:
            continue
        
        key = bn.partition('_')[0]
        if key not in positions_d:
            positions_d[key] = []
        positions_d[key].append(dname)
        
    dd = [len(val) for val in positions_d.values()]
    
    if not all(x==dd[0] for x in dd):
        import pdb
        pdb.set_trace()
        raise ValueError
    
    for pos, dnames in positions_d.items():
        try:
            _process_pos(main_dir, pos, dnames)
        except ValueError:
            continue
        

#%%
if __name__ == '__main__':
    #root_dir = '/Volumes/NTFS/Andrew Data/'
    #root_dir = '/run/media/ajaver/Loci II/Andrew Data' 
    #root_dir = '/run/media/ajaver/Loci II/TnaA Growth' 
    #root_dir = '/run/media/ajaver/Loci II/Nikon/Perturbations/Long Movies 271213' 
    #root_dir = '/run/media/ajaver/Loci II/Nikon/Perturbations/261213 Long Movies' 
    root_dir = '/run/media/ajaver/Loci I/Nikon/First_Set/'
    
    dnames = os.listdir(root_dir)
    dnames = [x for x in dnames if not 'FixedCells' in x]
    
    dnames = [x for x in dnames if not x in ['20140402_MRR_Gly_I20000_Andor']]
    
    bad_files = []
    for dname in dnames[::-1]:
        if not os.path.isdir(os.path.join(root_dir, dname)):
            continue
        
        print(dname)
        while True:
            try:
                _process_directory(os.path.join(root_dir, dname))
            except ValueError:
                bad_files.append(dname)
                print('?')
                #continue
            break
    print(bad_files)