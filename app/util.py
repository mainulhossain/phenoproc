from __future__ import print_function

import os
from os import path
from .models import DataSource, Data
from flask import current_app
import sys
from app.biowl.fileop import PosixFileSystem, HadoopFileSystem, GalaxyFileSystem

class Utility:
    @staticmethod
    def ValueOrNone(val):
        try:
            return int(val)
        except ValueError:
            return 0
        
    @staticmethod
    def get_rootdir(datasource_id):
        datasource = DataSource.query.get(datasource_id)
        if datasource_id == 1:
            return path.join(datasource.url, current_app.config['HDFS_DIR'])
        if datasource_id == 2:
            return path.join(datasource.url, current_app.config['DATA_DIR'])
        if datasource_id == 3:
            return path.join(datasource.url, '/')
        return ""
    
    @staticmethod
    def get_fullpath(data):
        #print(str(data_id), file=sys.stderr)
        if data is None:
            return ''
        return path.join(Utility.get_rootdir(data.datasource_id), data.url)
    
    @staticmethod
    def get_quota_path(path):
        if not path:
            path = current_app.config['PUBLIC_DIR']
        elif not path.startswith(current_app.config['PUBLIC_DIR']):
            path = os.path.join(current_app.config['CURRENT_USER'], path)
        return path
    
    @staticmethod
    def fs_by_prefix(path):
        if path:
            path = os.path.normpath(path)
            fs = path.split(os.sep)
            if not fs:
                return None
            
            dsid = 0
            if fs[0] == 'HDFS':
                dsid = 1
            elif fs[0] == 'GalaxyFS':
                dsid = 3
            else:
                dsid = 2 # fs[0] == 'LocalFS':
        else:
            dsid = 2 # LocalFS is default
        
        ds = DataSource.query.get(dsid)
        root = Utility.get_rootdir(ds.id)
        
        if dsid == 1:
            return HadoopFileSystem(ds.url, 'hdfs')
        elif dsid == 3:
            return GalaxyFileSystem(ds.url, '7483fa940d53add053903042c39f853a')
        else:
            return PosixFileSystem(root)
    
    @staticmethod
    def get_normalized_path(path):
        fs = Utility.fs_by_prefix(path)
        path = fs.strip_root(path)
        path = Utility.get_quota_path(path)
        return fs.normalize_path(path)