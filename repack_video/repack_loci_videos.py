#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 16 00:56:02 2018

@author: ajaver
"""

import os
import cv2
import pandas as pd
import tables
import numpy as np
import shutil
import glob
import scipy.io
import tqdm

is_debug = False

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
    images = np.array(images)
    return images


def _read_header(fname):
    try:
        mat = scipy.io.loadmat(fname, squeeze_me=True)
    except:
        return None
    
    assert mat['header'].size == 1
    header = pd.DataFrame(mat['header'], index=[0]).loc[0]
    return header

def _add_header_info(dataset, header):
    if header is None:
        return
    for h,v in zip(header.index, header.values):
        dataset._v_attrs[h] = v


def _process_directory(main_dir):
    dnames = os.listdir(main_dir)
    
    loci_dnames = [x for x in dnames if x.startswith('Before') or x.startswith('After')]
    phc_dir = os.path.join(main_dir, 'PhC')
    
    
    #illum_file = os.path.join(main_dir, 'Illumination.mat')
    #illum_vec = scipy.io.loadmat(illum_file, squeeze_me=True)
    
    for loci_dname in loci_dnames:
        loci_dname_f = os.path.join(main_dir, loci_dname)
        if not os.path.isdir(loci_dname_f) or not loci_dname_f.endswith('_Fluo'):
            continue
        
        
        save_name = loci_dname_f.replace('_Fluo', '.hdf5')
        
        #save_name = os.path.join(main_dir, pos + '.hdf5')
        if os.path.exists(save_name):
            try:
                with tables.File(save_name, 'r') as fid:
                    assert fid.get_node('/info')._v_attrs['has_finished'] == 1
            except (tables.exceptions.NoSuchNodeError):
                pass
            except:
                if is_debug:
                    import pdb
                    pdb.set_trace()
                    raise
                else:
                    continue
            
        
        
        fnames = os.listdir(loci_dname_f)
        img_files = [x for x in fnames if x.endswith('.tif')]
        if len(img_files) == 0:
            continue
        
        images = _read_img_list(loci_dname_f, img_files, desc = loci_dname)
        
        if any(x is None for x in images):
            continue
        
        print(save_name)
        with tables.File(save_name, 'w') as fid:
            
            
            gg = fid.create_carray(
                    '/',
                    'Fluo',
                    obj=images,
                    chunkshape = (1, *images.shape[1:]),
                    filters=TABLE_FILTERS)
            
            
            remainder_files = [x for x in fnames if not x.endswith('.tif')]
            if remainder_files:
                if not 'header.mat' in remainder_files:
                    print(remainder_files)
                    if is_debug:
                        import pdb
                        pdb.set_trace()
                        raise ValueError
                    else:
                        continue
                header = _read_header(os.path.join(loci_dname_f, 'header.mat'))
                _add_header_info(gg, header)
            
            
            phc_files = glob.glob(os.path.join(phc_dir, loci_dname.replace('_Fluo', '*.tif')))
            for fname in sorted(phc_files):
                h = _read_header(fname.replace('.tif', '_header.mat'))
                img = cv2.imread(fname, -1)
                
                ch_str = fname.rpartition('_')[-1][:-4]
                
                if ch_str == 'Fluo':
                    ch_str = ch_str + '_single'
                
                gg = fid.create_carray(
                    '/',
                    ch_str,
                    obj=img[None, ...],
                    chunkshape = (1, *img.shape),
                    filters=TABLE_FILTERS)
                _add_header_info(gg, h)
            
            fid.get_node('/Fluo')._v_attrs['has_finished'] = 1
        
            #remove processed files
            
            if remainder_files == ['header.mat']:
                shutil.rmtree(loci_dname_f)
            else:
                files2remove = [os.path.join(loci_dname_f,x) for x in img_files]
                files2remove.append(os.path.join(loci_dname_f, 'header.mat'))
                #do not remove the whole directory only the processed files
                for fname in files2remove:
                    os.remove(fname)
                    
                
            for fname in phc_files:
                hname = fname.replace('.tif', '_header.mat')
                os.remove(fname)
                
                if os.path.exists(hname):
                    os.remove(hname)

if __name__ == '__main__':
    #root_dir = '/Volumes/NTFS/Nikon/'
    root_dir = '/run/media/ajaver/Loci II/Nikon/'
    #not very nice but faster
    #dnames = [x for x in glob.glob(os.path.join(root_dir, '*', '*', '*')) if os.path.isdir(x)]
    #dnames = [x for x in glob.glob(os.path.join(root_dir, '*',  '*')) if os.path.isdir(x)]
    
    root_dir = '/run/media/ajaver/Loci I/Nikon/'
    dnames = [x for x in glob.glob(os.path.join(root_dir, '*', '*', '*')) if os.path.isdir(x)]
    
    #%%
    for dname in dnames[::-1]:
        print(dname)
        _process_directory(os.path.join(root_dir, dname))
        
        
        