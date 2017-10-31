from __future__ import print_function

from os import path
from .models import DataSource, Data
from flask import current_app
import sys

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