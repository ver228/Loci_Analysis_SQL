#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 30 10:20:29 2017

@author: ajaver
"""
import sqlite3
import os
from scipy.io import loadmat
import numpy as np

sql_create_db = '''
  CREATE TABLE IF NOT EXISTS `DB_CSVs` (
  `id` int(11) NOT NULL ,
  `file` varchar(255) NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
);


CREATE TABLE IF NOT EXISTS `growth_media` (
  `id` int(11) NOT NULL,
  `name` varchar(255) UNIQUE,
  `details` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `illumination_types` (
  `id` int(11) NOT NULL,
  `name` varchar(255) UNIQUE,
  `details` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `illuminations` (
  `id` int(11) NOT NULL,
  `illumination_type_id` int(11) NOT NULL,
  `value` float DEFAULT NULL,
  PRIMARY KEY (`id`),
  FOREIGN KEY(illumination_type_id) REFERENCES illumination_types(id)
);

CREATE TABLE IF NOT EXISTS `perturbation_types` (
  `id` int(11) NOT NULL,
  `name` varchar(255) UNIQUE,
  `details` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `perturbations` (
  `id` int(11) NOT NULL,
  `perturbation_type_id` int(11) NOT NULL,
  `concentration` float DEFAULT NULL,
  `is_fixed` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  FOREIGN KEY(perturbation_type_id) REFERENCES perturbation_types(id)
);


CREATE TABLE IF NOT EXISTS `loci_locations` (
  `id` int(11) NOT NULL,
  `position_raw` int(11) NULL,
  `position_centisome` float NULL,
  `distance_from_oriC` float NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `knockouts` (
  `id` int(11) NOT NULL,
  `name` varchar(255),
  PRIMARY KEY (`id`)
);


CREATE TABLE IF NOT EXISTS `strains` (
  `id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `loci_location_id`,
  `knockout_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  FOREIGN KEY(loci_location_id) REFERENCES loci_locations(id),
  FOREIGN KEY(knockout_id) REFERENCES knockouts(id)
);


CREATE TABLE IF NOT EXISTS `experiments` (
  `id` int(11) NOT NULL ,
  `DB_CSV_id` int(11) NOT NULL,
  `experiment_id_in_DB_CSV` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `date` date DEFAULT NULL,
  `strain_id` int(11) DEFAULT NULL,
  `perturbation_id` int(11) DEFAULT NULL,
  `illumination_id` int(11) DEFAULT NULL,
  `growth_medium_id` int(11) DEFAULT NULL,
  `delta_time` float DEFAULT NULL,
  `pixel_size` float DEFAULT NULL,
  PRIMARY KEY (`id`),
  FOREIGN KEY(DB_CSV_id) REFERENCES DB_CSVs(id),
  FOREIGN KEY(strain_id) REFERENCES strains(id),
  FOREIGN KEY(perturbation_id) REFERENCES perturbations(id),
  FOREIGN KEY(illumination_id) REFERENCES illuminations(id),
  FOREIGN KEY(growth_medium_id) REFERENCES growth_media(id)
);

CREATE TABLE IF NOT EXISTS `videos` (
  `id` int(11) NOT NULL,
  `file` varchar(255) NOT NULL,
  `experiment_id` int(11) NOT NULL,
  `DB_CSV_id` int(11) NOT NULL,
  `video_id_in_DB_CSV` int(11) NOT NULL,
  PRIMARY KEY (`id`)
  FOREIGN KEY(experiment_id) REFERENCES experiments(id)
);

CREATE TABLE IF NOT EXISTS `particles` (
  `id` int(11) NOT NULL,
  `video_id` int(11) NOT NULL,
  `particle_id_in_video` int(11) NOT NULL,
  UNIQUE(`video_id`, `particle_id_in_video`)
  PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `coordinates` (
  `particle_id` int(11) NOT NULL ,
  `video_frame` int(11) NOT NULL,
  `x_raw` float DEFAULT NULL,
  `y_raw` float DEFAULT NULL,
  `x_dedrift` float DEFAULT NULL,
  `y_dedrift` float DEFAULT NULL,
  `signal` float DEFAULT NULL,
  `background` float DEFAULT NULL,
  PRIMARY KEY (`particle_id`,`video_frame`),
  FOREIGN KEY(particle_id) REFERENCES particles(id)
);

CREATE TABLE IF NOT EXISTS `particle_MSDs` (
  `particle_id` int(11) NOT NULL ,
  `lag_time` int(11) NOT NULL,
  `MSD` float DEFAULT NULL,
  `estimated_error` float DEFAULT NULL,
  `MSD_minor_axis` float DEFAULT NULL,
  `MSD_major_axis` float DEFAULT NULL,
  FOREIGN KEY(particle_id) REFERENCES particles(id)
);

CREATE TABLE IF NOT EXISTS `particle_properties` (
  `particle_id` int(11) NOT NULL ,
  `initial_frame` int(11) NOT NULL,
  `length_frames` int(11) NOT NULL,
  `initial_signal` float DEFAULT NULL,
  `initial_background` float DEFAULT NULL,
  `scaling` float DEFAULT NULL,
  `prefactor` float DEFAULT NULL,
  `PCA_angle` float DEFAULT NULL,
  `PCA_eigenvalue_1` float DEFAULT NULL,
  `PCA_eigenvalue_2` float DEFAULT NULL,
  `eccentricity` float DEFAULT NULL,
  `asphericity` float DEFAULT NULL,
  `drift_velocity` float DEFAULT NULL,
  `drift_velocity_minor_axis` float DEFAULT NULL,
  `drift_velocity_major_axis` float DEFAULT NULL,
  `radius_of_gyration_major_axis` float DEFAULT NULL,
  `radius_of_gyration_minor_axis` float DEFAULT NULL,
  FOREIGN KEY(particle_id) REFERENCES particles(id)
);

CREATE TABLE IF NOT EXISTS `cells` (
  `id` int(11) NOT NULL ,
  `video_id` int(11) NOT NULL,
  `cell_id_in_video` int(11) NOT NULL,
  `area` float DEFAULT NULL,
  `length` float DEFAULT NULL,
  `width` float DEFAULT NULL,
  `center_of_mass_x` float DEFAULT NULL,
  `center_of_mass_y` float DEFAULT NULL,
  `angle` float DEFAULT NULL,
  `number_of_loci` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  FOREIGN KEY(video_id) REFERENCES videos(id)
);


CREATE TABLE IF NOT EXISTS `particle_coordinates_in_cell` (
  `particle_id` int(11) NOT NULL,
  `cell_id` int(11) NOT NULL,
  `position_minor_axis` float DEFAULT NULL,
  `position_major_axis` float DEFAULT NULL,
  FOREIGN KEY(particle_id) REFERENCES particles(id),
  FOREIGN KEY(cell_id) REFERENCES cells(id)
);
'''

def initialize_db(DB_path, DELETE_PREV=True):
    
    conn = sqlite3.connect(DB_path)
    cur = conn.cursor()
    if DELETE_PREV:
        all_tabs = [x.partition('(')[0].strip() for x in sql_create_db.split('CREATE TABLE IF NOT EXISTS') if x]
        for tab in all_tabs:
            if tab.strip():
                cur.execute('''DROP TABLE IF EXISTS {}'''.format(tab))
    
    for sql_tab in sql_create_db.split(';'):
        if sql_tab:
            cur.execute(sql_tab)
    
    conn.close()

def insert_experiments(DB_path_old, DB_path):
#%% insert
    conn_old = sqlite3.connect(DB_path_old)
    cur_old = conn_old.cursor()
    
    conn = sqlite3.connect(DB_path)
    cur = conn.cursor()
    cur.execute('PRAGMA foreign_keys = ON;')
    #%% DB_CSVs
    cur_old.execute('''SELECT * FROM DB_names''')
    data = cur_old.fetchall()
    cur.executemany('''INSERT OR REPLACE INTO `DB_CSVs` (id, file, name) VALUES (?,?,?)''', data)
    conn.commit()
    
    #%% growth media
    cur_old.execute('''SELECT * FROM growth_media''')
    data = cur_old.fetchall()
    data = _value_to_null(data)
    
    cur.executemany('''INSERT OR REPLACE INTO `growth_media` (id, name, details) VALUES (?,?,?)''', data)
    conn.commit()

    #%% illumination
    cur_old.execute('''SELECT * FROM illumination_types''')
    data = cur_old.fetchall()
    data = _value_to_null(data)
    cur.executemany('''INSERT OR REPLACE INTO `illumination_types` (id, name, details) VALUES (?,?,?)''', data)
    
    cur_old.execute('''SELECT distinct illumination_id, illumination_value FROM experiments;''')
    data = cur_old.fetchall()
    data = _add_ids(data)
    cur.executemany('''INSERT OR REPLACE INTO `illuminations` (id, illumination_type_id, value) VALUES (?,?,?)''', data)
    
    illumination_dict = {x[1:]:x[0] for x in data}
    #%% perturbation
    cur_old.execute('''SELECT * FROM perturbations''')
    data = cur_old.fetchall()
    data = _value_to_null(data)
    cur.executemany('''INSERT OR REPLACE INTO `perturbation_types` (id, name, details) VALUES (?,?,?)''', data)
    
    cur_old.execute('''SELECT distinct concentration FROM experiments;''')
    concentrations = cur_old.fetchall()
    perturbations = [(x, 0., 0) for x in [0] + list(range(1,4))] + \
    [(0,0,1)] + [(1, x, fix) for x, in concentrations for fix in range(2)]
    
    perturbations = _add_ids(perturbations)
    
    cur.executemany('''
    INSERT OR REPLACE INTO `perturbations` (id, perturbation_type_id, concentration, is_fixed) 
    VALUES (?,?,?,?)''', perturbations)
    conn.commit()
    
    cur.execute('''
    SELECT p.id, t.id, p.concentration, p.is_fixed 
    FROM perturbations AS p 
    JOIN perturbation_types AS t ON t.id=perturbation_type_id
    ''')
    data_new = cur.fetchall()
    
    perturbation_dict =  {x[1:]:x[0] for x in data_new}
    
    #%% strains
    #knockouts
    cur_old.execute('''SELECT * FROM knockouts''')
    data = cur_old.fetchall()
    data = _value_to_null(data)
    cur.executemany('''INSERT OR REPLACE INTO `knockouts` (id, name) VALUES (?,?)''', data)
    
    #loci_locations
    cur_old.execute('''SELECT strain_id,position_raw, position_centisome, distance_from_oriC  FROM strains''')
    data = cur_old.fetchall()
    data = [row if not all(x==0 for x in row[1:]) else tuple([row[0]] + [None]*3) for row in data]
    cur.executemany('''
    INSERT OR REPLACE INTO `loci_locations` (id, position_raw, position_centisome, distance_from_oriC) 
    VALUES (?,?,?,?)''', data)
    
    
    #strain
    cur_old.execute('''
    SELECT DISTINCT s.strain_name, k.knockout_name, e.strain_id, e.knockout_id
    FROM experiments AS e 
    JOIN strains AS s ON e.strain_id=s.strain_id
    JOIN knockouts AS k ON e.knockout_id=k.knockout_id;
    ''')
    data = cur_old.fetchall()
    
    strains = []
    for irow, row in enumerate(data):
        if row[1] != 'NA':
            name = row[0] + '+ D-' +row[1]
        else:
            name = row[0]
        
        new_row = irow, name, row[2], row[3]
        strains.append(new_row)
    
    cur.executemany('''
    INSERT OR REPLACE INTO `strains` (id, name, loci_location_id, knockout_id) 
    VALUES (?,?,?,?)''', strains)
    conn.commit()
    
    cur.execute('''SELECT loci_location_id, knockout_id FROM strains''')
    data_new = cur.fetchall()
    strain_dict =  {x:ii for ii,x in enumerate(data_new)}
    
    #%%experiments
    cur_old.execute('''SELECT * FROM experiments;''')
    data = cur_old.fetchall()
    
    experiments = []
    for row in data:        
        illumination_id = illumination_dict[(row[9], row[10])]
        strain_id = strain_dict[(row[5], row[6])]
        perturbation_id = perturbation_dict[(row[7], row[8], row[-3])]
        new_row = list(row[0:5]) + [strain_id, perturbation_id, illumination_id, row[9]] + list(row[-2:])
        experiments.append(tuple(new_row))
    
    sql = '''
    INSERT OR REPLACE INTO `experiments` (id, DB_CSV_id, experiment_id_in_DB_CSV, 
    name, date, strain_id, perturbation_id, illumination_id, growth_medium_id, 
    delta_time, pixel_size) 
    VALUES ({})'''.format(','.join(["?"]*len(experiments[0])))
    cur.executemany(sql, experiments)
    
    #%% videos
    cur_old.execute('''
    SELECT v.video_id, v.video_path, v.experiment_id, e.DB_id, v.DB_vid_id
    FROM videos AS v JOIN experiments AS e ON v.experiment_id=e.experiment_id''')
    data = cur_old.fetchall()
    cur.executemany('''
    INSERT OR REPLACE INTO `videos` (id, file, experiment_id, DB_CSV_id, video_id_in_DB_CSV) 
    VALUES (?,?,?,?,?)''', data)
    
    conn.commit()

def _insert_video_coord(conn, cur, root_dir, video_id):
    sql = '''
    SELECT d.name, v.video_id_in_DB_CSV
    FROM experiments AS e
    JOIN DB_CSVs AS d ON e.DB_CSV_id=d.id
    JOIN videos AS v ON v.experiment_id=e.id
    WHERE v.id={}
    '''.format(video_id)
    
    cur.execute(sql)
    out = cur.fetchall()
    DB_dir, DB_vid_id = out[0]
    
    cur.execute('''DELETE FROM coordinates 
        WHERE particle_id IN (SELECT id FROM particles WHERE video_id={})
        '''.format(video_id))
    cur.execute('''DELETE FROM particles WHERE video_id={}'''.format(video_id))
    conn.commit()
    
    cur.execute('SELECT max(id) FROM particles')
    out  = cur.fetchall()
    tot_particles, = out[0]
    if tot_particles is None:
        tot_particles = 0
    
    #DO NOT FORGET TO USE THE DEDRIFTED COORDINATES
    file_coord = os.path.join(root_dir, 'Results', DB_dir, 'TrackData_%i.mat' % DB_vid_id)
    assert os.path.exists(file_coord)
    
    file_coord_dedrift = os.path.join(root_dir, 'Results', DB_dir, 'dedrift_data', 'TrackData_%i.mat' % DB_vid_id)
    assert os.path.exists(file_coord)
    
    file_snr = os.path.join(root_dir, 'Results', DB_dir, 'SNR_%i.mat' % DB_vid_id)
    assert(file_snr)
    
    try:
        coord_data = loadmat(file_coord)
        snr_data = loadmat(file_snr)    
    except:
        print('ERROR', file_coord)
        return 0
    
    
    try:
        coord_data_dedrift = loadmat(file_coord_dedrift)
        xx_dedrift = coord_data_dedrift['positionsx'].data.tolist()
        yy_dedrift = coord_data_dedrift['positionsy'].data.tolist()
    except:
        print('ERROR', file_coord)
        return 0
    
    
    
    xx = coord_data['positionsx'].data.tolist()
    tt = coord_data['positionsy'].indices.astype(np.int).tolist()
    yy = coord_data['positionsy'].data.tolist()
    
    sig = snr_data['SNRStats']['signal'][0][0].data.tolist()
    bgnd = snr_data['SNRStats']['bgnd'][0][0].data.tolist()
    
    pp = np.zeros(len(tt), np.int)    
    indptr = coord_data['positionsx'].indptr
    for i in range(len(indptr)-1):
        pp[indptr[i]:indptr[i+1]] = i+1
    pp = pp.tolist()
    
    
    vid_particles_ids = np.unique(pp) 
    particles_ids = vid_particles_ids + tot_particles
    vid_particles_ids = vid_particles_ids.tolist()
    particles_ids = particles_ids.tolist()
    
    p_id_dict = {p_vid:p_id for p_id, p_vid in zip(particles_ids,vid_particles_ids)}
    
    coordinates_to_insert = []
    particles_to_insert = []
    
    for p_vid, p_id in p_id_dict.items():
        new_row = (p_id, video_id, p_vid)
        particles_to_insert.append(new_row)
        
    for p, t, x, y, x_d, y_d, s, b in zip(*[pp, tt, xx, yy, xx_dedrift, yy_dedrift, sig, bgnd]):
        coordinates_to_insert.append((p_id_dict[p], t, x,y, x_d, y_d, s,b))

    cur.executemany('''
    INSERT INTO `particles` (id, video_id, particle_id_in_video) 
    VALUES (?,?,?)''', particles_to_insert)
    
    cur.executemany('''
    INSERT INTO `coordinates` (particle_id, video_frame, x_raw, y_raw, x_dedrift, y_dedrift, signal, background) 
    VALUES (?,?,?,?,?,?,?,?)''', coordinates_to_insert)
    conn.commit()
    
    return len(vid_particles_ids)

def insert_coordinates(DB_path, IGNORE_INSERTED = True):

    conn = sqlite3.connect(DB_path)
    cur = conn.cursor()
    cur.execute('PRAGMA foreign_keys = ON;')
    
    
    sql_find_videos = '''
    SELECT id FROM videos'''
    
    if IGNORE_INSERTED:
        sql_find_videos += '''
        WHERE id NOT IN (SELECT DISTINCT video_id FROM particles)
        '''
    cur.execute(sql_find_videos)
    videos_ids = cur.fetchall()
    
    
    tot_videos = max([x[0] for x in videos_ids])
    for video_id, in videos_ids:
        n_inserted = _insert_video_coord(conn, cur, root_dir, video_id)        
        print('{} of {}) -> {}'.format(video_id, tot_videos, n_inserted))
    
    cur.close()
    conn.close()  
    
def _value_to_null(data, val2chg='NA'):
    return [tuple(x if x!='NA' else None for x in row) for row in data]

def _add_ids(data):
    return [tuple([irow] + list(row)) for (irow, row) in enumerate(data)]

def _insert_exp_MSDs(conn, cur, root_dir, experiment_id):
    #%%
    sql = '''
    SELECT d.name, e.experiment_id_in_DB_CSV
    FROM experiments AS e
    JOIN DB_CSVs AS d ON e.DB_CSV_id=d.id
    WHERE e.id={}
    '''.format(experiment_id)
    
    cur.execute(sql)
    out = cur.fetchall()
    DB_dir, DB_exp_id = out[0]
    
    file_msd = os.path.join(root_dir, 'Summary', DB_dir, 'Data_MME_%i.mat' % DB_exp_id)
    file_msd_ang = os.path.join(root_dir, 'Summary', DB_dir, 'Data_MSD_Ang_%i.mat' % DB_exp_id)
    file_msd_shape = os.path.join(root_dir, 'Summary', DB_dir, 'Data_MSD_Shape_%i.mat' % DB_exp_id)
    file_int = os.path.join(root_dir, 'Summary', DB_dir, 'Properties_Int_%i.mat' % DB_exp_id)
    
    try:
        msd_data = loadmat(file_msd)
        msd_ang_data = loadmat(file_msd_ang)
        int_data = loadmat(file_int)
    except:
        print('ERROR', file_msd)
        return (0,0)
    
    
    trackID = msd_data['trackID']
    
    sql = '''
    SELECT p.id, v.video_id_in_DB_CSV, p.particle_id_in_video
    FROM experiments AS e
    JOIN DB_CSVs AS d ON e.DB_CSV_id=d.id
    JOIN videos AS v ON v.experiment_id=e.id
    JOIN particles AS p ON p.video_id=v.id
    WHERE e.id = {}
    '''.format(experiment_id)
    
    cur.execute(sql)
    videos_ids = cur.fetchall()
    
    particle_dict = {(x[1], x[2]):x[0] for x in videos_ids}
    dd = [tuple(map(int, x)) for x in trackID.T]
    try:
        particle_id = np.array([particle_dict[x] for x in dd])
    except (KeyError):
        print('ERROR', file_msd)
        return (0,0)
        
    initial_frame = int_data['IntProps']['iniFrame'][0,0]
    length_frames = msd_data['timeAv']['Npoints'][0,0]
    initial_signal = int_data['IntProps']['sig'][0,0]['iniM'][0,0]
    initial_background = int_data['IntProps']['bgnd'][0,0]['iniM'][0,0]
    
    scaling = msd_data['alpha10']['coeff'][0,0]
    if 'gamma' in msd_data['alpha10'].dtype.names:
        prefactor = msd_data['alpha10']['gamma'][0,0]
    else:
        prefactor =  np.full(scaling.shape, np.nan)
    
    PCA_angle = msd_ang_data['trackAngDat']['angle'][0,0]
    PCA_eigenvalue_1 = msd_ang_data['trackAngDat']['majorAxis'][0,0]
    PCA_eigenvalue_2 = msd_ang_data['trackAngDat']['minorAxis'][0,0]
    eccentricity = msd_ang_data['trackAngDat']['eccentricity'][0,0]
    asphericity = msd_ang_data['trackAngDat']['A2'][0,0]
    drift_velocity = msd_ang_data['trackAngDat']['vel_drift'][0,0]
    
    MSD = msd_data['timeAv']['MSD'][0,0]
    lag_time = np.repeat(np.arange(1, MSD.shape[0]+1)[:, np.newaxis], MSD.shape[1], 1)
    estimated_error = msd_data['timeAv']['errMat'][0,0]
    
    if os.path.exists(file_msd_shape):
        msd_shape_data = loadmat(file_msd_shape)
        
        MSD_minor_axis = msd_shape_data['MSD_Shape']['mA'][0,0]
        MSD_major_axis = msd_shape_data['MSD_Shape']['MA'][0,0]
        
        drift_velocity_minor_axis = msd_shape_data['MSD_Shape']['v_mA'][0,0]
        drift_velocity_major_axis = msd_shape_data['MSD_Shape']['v_MA'][0,0]
        radius_of_gyration_major_axis = msd_shape_data['MSD_Shape']['Rg_mA'][0,0]
        radius_of_gyration_minor_axis = msd_shape_data['MSD_Shape']['Rg_MA'][0,0]
    else:
        MSD_minor_axis = np.full(MSD.shape, np.nan)
        MSD_major_axis = np.full(MSD.shape, np.nan)
        
        drift_velocity_minor_axis = np.full(initial_frame.shape, np.nan)
        drift_velocity_major_axis = np.full(initial_frame.shape, np.nan)
        radius_of_gyration_major_axis = np.full(initial_frame.shape, np.nan)
        radius_of_gyration_minor_axis = np.full(initial_frame.shape, np.nan)
    
    def _fix_rows(data):
        dd = [[None if np.isnan(n) or isinstance(n, complex) else n for n in x.flatten().tolist()] for x in data]
        return list(zip(*dd))
        
    
    particle_ids = particle_ids = np.repeat(particle_id.T[np.newaxis, :], MSD.shape[0], 0)
    particle_MSDs = [particle_ids, lag_time, MSD, estimated_error, MSD_minor_axis, MSD_major_axis]
    
    particle_properties = [particle_id, initial_frame, length_frames, 
                           initial_signal, initial_background, scaling, prefactor,
                           PCA_angle, PCA_eigenvalue_1, PCA_eigenvalue_2, 
                           eccentricity, asphericity, drift_velocity, 
                           drift_velocity_minor_axis, drift_velocity_major_axis,
                           radius_of_gyration_major_axis, radius_of_gyration_minor_axis]
    
    particle_MSDs = _fix_rows(particle_MSDs)
    particle_properties = _fix_rows(particle_properties)
    
    
    sql_part_props = '''
    INSERT INTO `particle_properties` (particle_id, initial_frame, length_frames, 
                           initial_signal, initial_background, scaling, prefactor,
                           PCA_angle, PCA_eigenvalue_1, PCA_eigenvalue_2, 
                           eccentricity, asphericity, drift_velocity, 
                           drift_velocity_minor_axis, drift_velocity_major_axis,
                           radius_of_gyration_major_axis, radius_of_gyration_minor_axis) 
    VALUES ({})'''.format(','.join(["?"]*len(particle_properties[0])))

    sql_part_msd = '''
    INSERT INTO `particle_MSDs` (particle_id, lag_time, MSD, estimated_error, MSD_minor_axis, MSD_major_axis) 
    VALUES ({})'''.format(','.join(["?"]*len(particle_MSDs[0])))
    
    sql = '''
    DELETE FROM particle_MSDs
    WHERE particle_id IN (
    SELECT p.id
    FROM experiments AS e
    JOIN videos AS v ON v.experiment_id=e.id
    JOIN particles AS p ON p.video_id=v.id
    WHERE e.id = {})
    '''.format(experiment_id)
    cur.execute(sql)
    
    sql = '''
    DELETE FROM particle_properties
    WHERE particle_id IN (
    SELECT p.id
    FROM experiments AS e
    JOIN videos AS v ON v.experiment_id=e.id
    JOIN particles AS p ON p.video_id=v.id
    WHERE e.id = {})
    '''.format(experiment_id)
    cur.execute(sql)
    
    try:
        cur.executemany(sql_part_props, particle_properties)
        cur.executemany(sql_part_msd, particle_MSDs)
    except:
        import pdb
        pdb.set_trace()
    conn.commit()
    
    return (len(particle_properties), len(particle_MSDs))

def insert_MSDs(DB_path, IGNORE_INSERTED = True):
    conn = sqlite3.connect(DB_path)
    cur = conn.cursor()
    cur.execute('PRAGMA foreign_keys = ON;')
    
    sql_find_experiments = '''SELECT DISTINCT v.experiment_id
                            FROM videos AS v
                            JOIN particles AS p ON p.video_id=v.id''' 
    if IGNORE_INSERTED:
        sql_find_experiments += '''
        WHERE v.id NOT IN (
        SELECT DISTINCT v.id 
        FROM particle_properties AS pp
        JOIN particles AS p ON pp.particle_id=p.id
        JOIN videos AS v ON p.video_id=v.id)
        '''
    
    cur.execute(sql_find_experiments)
    exp_ids = cur.fetchall()
    tot_exp = max(x for x, in exp_ids)
    
    
    for experiment_id, in exp_ids:
        n_inserted = _insert_exp_MSDs(conn, cur, root_dir, experiment_id)
        print('{} of {}) -> {}'.format(experiment_id, tot_exp, n_inserted))
        
    conn.close()
        
if __name__ == '__main__':
    root_dir = '/Volumes/WormData/Loci_data/Tracking_Results/'
    
    
    DB_path = os.path.join(root_dir, 'loci_data_new.db')
    DB_path_old = os.path.join(root_dir, 'loci_data.db')
    
#    initialize_db(DB_path)
#    insert_experiments(DB_path_old, DB_path)
    insert_coordinates(DB_path, IGNORE_INSERTED = True)
    insert_MSDs(DB_path, IGNORE_INSERTED = True)
    

         