from __future__ import print_function
from os import makedirs

# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.

__author__ = "Mainul Hossain"
__date__ = "$Dec 10, 2016 2:23:14 PM$"

from flask import current_app
from abc import ABCMeta, abstractmethod
import os
import sys
from .models import DataType, DataSource
import json
import shutil
import tempfile

try:
    from hdfs import InsecureClient
except:
    pass

# class FileSystem(metaclass=ABCMeta):
#     @abstractmethod
#     def make_tree(self, path, recursive = true):
#         pass
#     
#     @abstractmethod
#     def makedirs(self, path):
#         pass

class PosixFileSystem(object):
#     def make_tree(self, recursive = true):
#         tree = dict(name=os.path.basename(path), children=[])
#         try: lst = os.listdir(path)
#         except OSError:
#             pass #ignore errors
#         else:
#             for name in lst:
#                 fn = os.path.join(path, name)
#                 if os.path.isdir(fn) and recursive:
#                     tree['children'].append(make_fs_tree(fn))
#                 else:
#                     tree['children'].append(dict(name=name))
#         return tree

    def make_tree(self, datasourceid, path):
        #tree = dict(name=os.path.basename(path), children=[])
        tree = dict(name=(os.path.basename(path), datasourceid + os.path.sep + path), children=[])
        try: lst = os.listdir(path)
        except OSError:
            pass #ignore errors
        else:
            for name in lst:
                fn = os.path.join(path, name)
                if os.path.isdir(fn):
                    tree['children'].append(make_tree(datasourceid, fn))
                else:
                    tree['children'].append({'name' : (name, datasourceid + os.path.sep + fn), 'children' : []})
        return tree
    
            
    def make_json(self, datasourceid, base, relative_path):
        path = os.path.join(base, relative_path)
        data_json = {'datasource': datasourceid, 'path': relative_path, 'name': os.path.basename(relative_path) }
        if os.path.isdir(path):
            data_json['type'] = DataType.Folder
            data_json['children'] = [self.make_json(datasourceid, base, os.path.join(relative_path, fn)) for fn in os.listdir(path)]
        else:
            data_json['type'] = DataType.File
        #print(json.dumps(data_json))
        return data_json
    
    def makedirs(self, path):
        if not os.path.exists(path):
            os.makedirs(path) 
        return path
    
    def delete(self, path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)
            
    def addfolder(self, path):
        i = 0
        while os.path.exists(os.path.join(path, "New Folder ({0})".format(i))):
            i += 1
        return self.makedirs(os.path.join(path, "New Folder ({0})".format(i)))
    
    def rename(self, oldpath, newpath):
        os.rename(oldpath, newpath)
        
    def saveUpload(self, file, fullpath):
        file.save(fullpath)
    
class HadoopFileSystem(object):
    def __init__(self, *opts):
        self.client = InsecureClient(current_app.config['WEBHDFS_ADDR'], user=current_app.config['WEBHDFS_USER'])
         
#     def make_tree(self, datasourceid, client, path):
#         tree = dict(name=(os.path.basename(path), datasourceid + os.path.sep + path), children=[])
#         try: lst = client.list(path, status=True)
#         except:
#             pass #ignore errors
#         else:
#             for fsitem in lst:
#                 fn = os.path.join(path, fsitem[0])
#                 if fsitem[1]['type'] == "DIRECTORY":
#                     tree['children'].append(make_hdfs_tree(datasourceid, client, fn))
#                 else:
#                     tree['children'].append({'name' : (fsitem[0], datasourceid + os.path.sep + fn), 'children' : []})
#         return tree

    def make_json(self, datasourceid, base, relative_path):
        path = os.path.join(base, relative_path)
        data_json = {'datasource': datasourceid, 'path': relative_path, 'name': os.path.basename(relative_path) }
        status = self.client.status(path, False)

        if status is not None:
            if status['type'] == "DIRECTORY":
                data_json['type'] = DataType.Folder
                data_json['children'] = [self.make_json(datasourceid, base, os.path.join(relative_path, fn)) for fn in self.client.list(path)]
            else:
                data_json['type'] = DataType.File
        #print(json.dumps(data_json))
        return data_json
    
    def makedirs(self, path):
        try: 
            client.makedirs(path)
        except:
            return None
        return path
    
    def delete(self, path):
        try: 
            if client.status(path, False) is not None:
                client.delete(path, true)
        except:
            pass
        return
        
    def addfolder(self, path):
        i = 0
        while client.status(os.path.join(path, "New Folder ({0})".format(i)), False) is None:
            i += 1
        return self.makedirs(os.path.join(path, "New Folder ({0})".format(i)))
    
    def rename(self, oldpath, newpath):
        try:
            client.rename(oldpath, newpath)
        except:
            pass
    
    def saveUpload(self, file, fullpath):
        localpath = tempfile.TemporaryFile() #os.path.join(tempfile.gettempdir(), os.path.basename(fullpath))
        try:
            file.save(localpath)
            client.upload(fullpath, localpath, True)
        except:
            pass
                
def getFileSystem(datasource_id):
    if datasource_id is None:
        return None    
    datasource = DataSource.query.get(datasource_id)
    if datasource is None:
        return None
    if datasource_id == 1:
        return HadoopFileSystem()
    elif datasource.id == 2:
        return PosixFileSystem()
    else:
        return None
        
if __name__ == "__main__":
    print("Hello World")
