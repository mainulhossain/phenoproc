from os import path
from .models import DataSource
from flask import current_app

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
