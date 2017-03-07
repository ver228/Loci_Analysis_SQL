#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  7 18:54:48 2017

@author: ajaver
"""
import sqlite3
import os


if __name__ == '__main__' :
    main_dir = '/Volumes/behavgenom_archive$/Avelino/Others/'
    db_name = os.path.join(main_dir, 'loci_data_new.db')
    conn = sqlite3.connect(db_name)
    
    sql = '''SELECT e.id, e.name, g.name
    FROM experiments AS e
    JOIN growth_media AS g ON g.id=e.growth_medium_id
    '''
    
    cur = conn.cursor()
    cur.execute(sql)
    all_exp = cur.fetchall()
    
    
    
    
    
    sql = 'select id,name from growth_media;'
    cur.execute(sql)
    all_media = cur.fetchall()
    
    media_dict = {y:x for x,y in all_media}
    
    real_media = [dat[1].split('_')[0] for dat in all_exp]
    new_medium_keys = [media_dict[x] if x in media_dict else media_dict[None] for x in real_media]
    pairs2update = [(row[0], nk) for row,nk in zip(all_exp, new_medium_keys)]
    
    for exp_id, media_key in pairs2update:
        sql = '''
        UPDATE experiments
        SET growth_medium_id = {}
        WHERE id = {}
        '''.format(media_key, exp_id)
        cur.execute(sql)
    
    conn.commit()