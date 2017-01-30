#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 30 10:20:29 2017

@author: ajaver
"""
root_dir = '/Volumes/WormData/Loci_data/Tracking_Results/'

DB_path_old = '/Volumes/WormData/Loci_data/Tracking_Results/loci_data.db'
DB_path = '/Volumes/WormData/Loci_data/Tracking_Results/loci_data_new.db'



'''CREATE TABLE `DB_CSVs` (
  `id` int(11) NOT NULL ,
  `file` varchar(255) NOT NULL,
  `name` varchar(255) NOT NULL
  PRIMARY KEY (`id`)
);'''


'''
CREATE TABLE `videos` (
  `id` int(11) NOT NULL,
  `file` varchar(255) NOT NULL,
  `experiment_id` int(11) NOT NULL,
  `DB_CSV_id` int(11) NOT NULL,
  `video_id_in_DB_CSV` int(11) NOT NULL,
  PRIMARY KEY (`video_id`)
);'''

'''
CREATE TABLE `experiments` (
  `id` int(11) NOT NULL ,
  `name` varchar(255) NOT NULL,
  `date` date DEFAULT NULL,
  `DB_CSV_id` int(11) NOT NULL,
  `experiment_id_in_DB_CSV` int(11) NOT NULL,
  `strain_id` int(11) DEFAULT NULL,
  `perturbation_id` int(11) DEFAULT NULL,
  `illumination_id` int(11) DEFAULT NULL,
  `growth_medium_id` int(11) DEFAULT NULL,
  `delta_time` float DEFAULT NULL,
  `pixel_size` float DEFAULT NULL,
  PRIMARY KEY (`experiment_id`)
);
'''

'''
CREATE TABLE `particles` (
  `id` int(11) NOT NULL,
  `video_id` int(11) NOT NULL,
  `particle_id_in_video` int(11) NOT NULL,
  PRIMARY KEY (`particle_id`)
);
'''

'''
CREATE TABLE `growth_media` (
  `id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `details` varchar(255) NOT NULL
);
'''


'''CREATE TABLE `illuminations` (
  `illumination_type_id` int(11) NOT NULL,
  `illumination_value` float DEFAULT NULL,
);
'''

'''CREATE TABLE `illumination_types` (
  `id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `details` varchar(255) NOT NULL
);
'''

'''CREATE TABLE `perturbation_types` (
  `id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `details` varchar(255) NOT NULL
);'''


'''CREATE TABLE `perturbations` (
  `perturbation_type_id` int(11) NOT NULL,
  `concentration` float DEFAULT NULL
  `is_fixed` tinyint(1) DEFAULT NULL,
);
'''

        
'''
CREATE TABLE `strains` (
  `id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `loci_location_id`,
  `knockout` varchar(255) NOT NULL,
);
'''

''' 
CREATE TABLE `loci_locations`
  `id` int(11) NOT NULL,
  `position_raw` int(11) NULL,
  `position_centisome` float NULL,
  `distance_from_oriC` float NULL,
'''


'''
CREATE TABLE `coordinates` (
  `particle_id` int(11) NOT NULL ,
  `video_frame` int(11) NOT NULL,
  `x` float DEFAULT NULL,
  `y` float DEFAULT NULL,
  `signal` float DEFAULT NULL,
  `background` float DEFAULT NULL,
  PRIMARY KEY (`particle_id`,`video_frame`)
)
'''




