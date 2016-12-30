# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.

__author__ = "Mainul Hossain"
__date__ = "$Dec 10, 2016 2:23:14 PM$"

from flask import current_app
from abc import ABCMeta, abstractmethod
import os
import sys
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
separator = ';'
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
        tree = dict(name=(os.path.basename(path), datasourceid + separator + path), children=[])
        try: lst = os.listdir(path)
        except OSError:
            pass #ignore errors
        else:
            for name in lst:
                fn = os.path.join(path, name)
                if os.path.isdir(fn):
                    tree['children'].append(make_fs_tree(datasourceid, fn))
                else:
                    tree['children'].append({'name' : (name, datasourceid + separator + fn), 'children' : []})
        return tree
    
    def makedirs(self, path):
        if not os.path.exists(path):
            os.makedirs(path) 
        return
    
class HadoopFileSystem(object):
    def __init__(self, *opts):
        self.client = InsecureClient(current_app.config['WEBHDFS_ADDR'], user=current_app.config['WEBHDFS_USER'])
        
#     def make_tree(self, path):
#         tree = dict(name=os.path.basename(path), children=[])
#         try:
#             lst = self.client.list(path, status=True)
#         except:
#             pass #ignore errors
#         else:
#             for name in lst:
#                 fn = os.path.join(path, name[0])
#                 if name[1]['type'] == "DIRECTORY" and recursive:
#                     tree['children'].append(make_hdfs_tree(client, fn))
#                 else:
#                     tree['children'].append(dict(name=name[0]))
#         return tree
    
    def make_tree(self, datasourceid, client, path):
        tree = dict(name=(os.path.basename(path), datasourceid + separator + path), children=[])
        try: lst = client.list(path, status=True)
        except:
            pass #ignore errors
        else:
            for fsitem in lst:
                fn = os.path.join(path, fsitem[0])
                if fsitem[1]['type'] == "DIRECTORY":
                    tree['children'].append(make_hdfs_tree(datasourceid, client, fn))
                else:
                    tree['children'].append({'name' : (fsitem[0], datasourceid + separator + fn), 'children' : []})
        return tree

    def makedirs(self, path):
        try: 
            client.makedirs(path)
        except:
            pass
        return

if __name__ == "__main__":
    print("Hello World")
