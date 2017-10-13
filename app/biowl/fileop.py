from __future__ import print_function

from os import listdir
from os import makedirs
import os
from os.path import isfile, join, isdir, abspath, dirname
import shutil
import sys
import tempfile
from urllib.parse import urlparse, urlunparse, urlsplit, urljoin

__author__ = "Mainul Hossain"
__date__ = "$Dec 10, 2016 2:23:14 PM$"


try:
    from hdfs import InsecureClient
except:
    pass

class PosixFileSystemBase():
    
    def __init__(self):
        self.localdir = join(abspath(dirname(__file__)), 'storage')
    
    def normalize_path(self, path):
        path = os.path.normpath(path)
        if path and path[0] == os.sep:
             path = path[1:]
        return join(self.localdir, path)
    
    def strip_root(self, path):
        if not path.startswith(self.localdir):
            return path
        return path[len(self.localdir):]
            
    def create_folder(self, path):
        path = self.normalize_path(path)
        if not os.path.exists(path):
            os.makedirs(path) 
        return path
    
    def remove(self, path):
        path = self.normalize_path(path)
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)
               
    def rename(self, oldpath, newpath):
        oldpath = self.normalize_path(oldpath)
        newpath = self.normalize_path(newpath)
        os.rename(oldpath, newpath)
    
    def get_files(self, path):
        path = self.normalize_path(path)
        return [f for f in listdir(path) if os.path.isfile(join(path, f))]
    
    def get_folders(self, path):
        path = self.normalize_path(path)
        return [f for f in listdir(path) if isdir(join(path, f))]
    
    def read(self, path):
        path = self.normalize_path(path)
        with open(path) as reader:
            return reader.read().decode('utf-8')
        
    def write(self, path, content):
        path = self.normalize_path(path)
        with open(path, 'w') as writer:
            return writer.write(content)
        
    def unique_filename(self, path, prefix, ext):
        make_fn = lambda i: os.path.join(path, '{0}({1}).{2}'.format(prefix, i, ext))

        for i in range(1, sys.maxsize):
            uni_fn = make_fn(i)
            if not os.path.exists(uni_fn):
                return uni_fn
    
    def exists(self, path):
        os.path.exists(self.normalize_path(path))
        
    def isdir(self, path):
        return os.path.isdir(self.normalize_path(path))
    
    def isfile(self, path):
        return os.path.isfile(self.normalize_path(path))
    
    def make_json(self, path):
        normalize_path = self.normalize_path(path)
        data_json = { 'path': path, 'text': os.path.basename(path) }
        data_json['folder'] = os.path.isdir(normalize_path)
        
        if os.path.isdir(normalize_path):
           data_json['nodes'] = [self.make_json(os.path.join(path, fn)) for fn in os.listdir(normalize_path)]
        return data_json

    def save_upload(self, file, fullpath):
        if self.isfile(fullpath):
            path = os.path.dirname(fullpath)
        file.save(os.path.join(fullpath, file.filename))
    
    def download(self, fullpath):
        if os.path.isfile(fullpath):
            return fullpath
        else:
            return None

class PosixFileSystem():
    
    def __init__(self, root = join(abspath(dirname(__file__)), 'storage')):
        self.localdir = root
        self.prefix = 'LocalFS'
        
    def normalize_path(self, path):
        path = os.path.normpath(path)
        if path.startswith(self.prefix):
            path = path[len(self.prefix):]
            
        while path and path[0] == os.sep:
            path = path[1:]
             
        return os.path.join(self.localdir, path)
    
    def make_prefix(self, path):
        if path.startswith(self.prefix):
            return path
        if path.startswith(self.localdir):
           path = path[len(self.localdir):]
        if not path.startswith(os.sep):
            path = os.sep + path
        return self.prefix + path
        
    def strip_root(self, path):
        if not path.startswith(self.localdir):
            return path
        return path[len(self.localdir):]
            
    def create_folder(self, path):
        path = self.normalize_path(path)
        if not os.path.exists(path):
            os.makedirs(path) 
        return self.make_prefix(path)
    
    def remove(self, path):
        path = self.normalize_path(path)
        dirpath = os.path.dirname(path)
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)
        return self.make_prefix(dirpath)
               
    def rename(self, oldpath, newpath):
        oldpath = self.normalize_path(oldpath)
        newpath = self.normalize_path(newpath)
        os.rename(oldpath, newpath)
        return self.make_prefix(newpath)
    
    def get_files(self, path):
        path = self.normalize_path(path)
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    
    def get_folders(self, path):
        path = self.normalize_path(path)
        return [f for f in os.listdir(path) if os.path.isdir(os.path.join(path, f))]
    
    def read(self, path):
        path = self.normalize_path(path)
        with open(path) as reader:
            return reader.read().decode('utf-8')
        
    def write(self, path, content):
        path = self.normalize_path(path)
        with open(path, 'w') as writer:
            return writer.write(content)
        
    def unique_filename(self, path, prefix, ext):
        make_fn = lambda i: os.path.join(self.normalize_path(path), '{0}({1}).{2}'.format(prefix, i, ext))

        for i in range(1, sys.maxsize):
            uni_fn = make_fn(i)
            if not os.path.exists(uni_fn):
                return uni_fn
    
    def exists(self, path):
        os.path.exists(self.normalize_path(path))
        
    def isdir(self, path):
        return os.path.isdir(self.normalize_path(path))
    
    def isfile(self, path):
        return os.path.isfile(self.normalize_path(path))
    
    def make_json(self, path):
        normalized_path = self.normalize_path(path)
        data_json = { 'path': self.make_prefix(path), 'text': os.path.basename(path) }
        data_json['folder'] = os.path.isdir(normalized_path)
        
        if os.path.isdir(normalized_path):
           data_json['nodes'] = [self.make_json(os.path.join(path, fn)) for fn in os.listdir(normalized_path)]
        return data_json

    def save_upload(self, file, path):
        path = self.normalize_path(path)
        if self.isfile(path):
            path = os.path.dirname(path)
        file.save(os.path.join(path, file.filename))
        self.make_prefix(os.path.join(path, file.filename))
    
    def download(self, path):
        path = self.normalize_path(path)
        if os.path.isfile(path):
            return path
        else:
            return None
                                   
class HadoopFileSystemBase():
    def __init__(self, url, user):
        u = urlsplit(url)
        if u.scheme != 'http' and u.scheme != 'https':
            raise "Invalid name node address"
        
        self.url = urlunparse((u.scheme, u.netloc, '', '', '', ''))
        self.client = InsecureClient(self.url, user=user)
        self.localdir = u.path
        self.prefix = 'HDFS'
    
    def normalize_path(self, path):
        path = self.strip_root(path)
        path = os.path.normpath(path)
        if path and path[0] == os.sep:
             path = path[1:]
        return os.path.join(self.localdir, path)
    
    def strip_root(self, path):
        if path.startswith(self.url):
            path = path[len(self.url):]
            if not path.startswith(self.localdir):
                raise 'Invalid hdfs path. It must start with the root directory'
        
        if not path.startswith(self.localdir):
            return path
        return path[len(self.localdir):]
        
    def create_folder(self, path):
        try:
            path = self.normalize_path(path)
            self.client.makedirs(path)
        except:
            return None
        return path
    
    def remove(self, path):
        try: 
            path = self.normalize_path(path)
            if self.client.status(path, False) is not None:
                self.client.delete(path, True)
        except Exception as e: print(e)
           
    def rename(self, oldpath, newpath):
        try:
            oldpath = self.normalize_path(oldpath)
            newpath = self.normalize_path(newpath)
            self.client.rename(oldpath, newpath)
        except Exception as e:
            print(e)
    
    def get_files(self, path):
        path = self.normalize_path(path)
        files = []
        for f in self.client.list(path):
            status = self.client.status(join(path, f), False)
            if status['type'] != "DIRECTORY":
                files.append(f)
        return files
    
    def get_folders(self, path):
        path = self.normalize_path(path)
        folders = []
        for f in self.client.list(path):
            status = self.client.status(join(path, f), False)
            if status['type'] == "DIRECTORY":
                folders.append(f)
        return folders
    
    def exists(self, path):
        path = self.normalize_path(path)
        status = self.client.status(path, False)
        return not (status is None)
        
    def isdir(self, path):
        path = self.normalize_path(path)
        status = self.client.status(path, False)
        return status['type'] == "DIRECTORY"
    
    def isfile(self, path):
        path = self.normalize_path(path)
        status = self.client.status(path, False)
        return status['type'] == "FILE"
        
    def read(self, path):
        path = self.normalize_path(path)
        with self.client.read(path) as reader:
            return reader.read().decode('utf-8')
    
    def write(self, path, content):
        path = self.normalize_path(path)
        self.client.write(path, content)
    
    def make_json(self, path):
        normalized_path = self.normalize_path(path)
        data_json = { 'path': urljoin(self.url, normalized_path), 'text': os.path.basename(path) }
        status = self.client.status(normalized_path, False)

        if status is not None:
            data_json['folder'] = status['type'] == "DIRECTORY"
            if status['type'] == "DIRECTORY":
                data_json['nodes'] = [self.make_json(os.path.join(path, fn)) for fn in self.client.list(normalized_path)]
        #print(json.dumps(data_json))
        return data_json
     
    def save_upload(self, file, fullpath):
        localpath = os.path.join(tempfile.gettempdir(), os.path.basename(fullpath))
        if os.path.isfile(localpath):
            os.remove(localpath)
        try:
            file.save(localpath)
            if isfile(fullpath):
                fullpath = os.path.dirname(fullpath)
            self.client.upload(self.normalize_path(fullpath), localpath, True)
        except:
            pass
        
    def download(self, fullpath):
        status = self.client.status(fullpath, False)
        if status is not None and status['type'] == "FILE":
            localpath = os.path.join(tempfile.gettempdir(), os.path.basename(fullpath))
            return self.client.download(self.normalize_path(fullpath), localpath, True)
        else:
            return None

class HadoopFileSystem():
    def __init__(self, url, user):
        u = urlsplit(url)
        if u.scheme != 'http' and u.scheme != 'https':
            raise "Invalid name node address"
        
        self.url = urlunparse((u.scheme, u.netloc, '', '', '', ''))
        self.client = InsecureClient(self.url, user=user)
        self.localdir = u.path
        self.prefix = 'HDFS'
    
    def normalize_path(self, path):
        path = os.path.normpath(path)
        if path.startswith(self.prefix):
            path = path[len(self.prefix):]
        while path and path[0] == os.sep:
            path = path[1:]
        return os.path.join(self.localdir, path)
    
    def strip_root(self, path):
        if path.startswith(self.url):
            path = path[len(self.url):]
            if not path.startswith(self.localdir):
                raise 'Invalid hdfs path. It must start with the root directory'
      
        if not path.startswith(self.localdir):
            return path
        return path[len(self.localdir):]
        
    def create_folder(self, path):
        try:
            path = self.normalize_path(path)
            self.client.makedirs(path)
        except:
            return None
        return path
    
    def remove(self, path):
        try: 
            path = self.normalize_path(path)
            if self.client.status(path, False) is not None:
                self.client.delete(path, True)
        except Exception as e: print(e)
           
    def rename(self, oldpath, newpath):
        try:
            oldpath = self.normalize_path(oldpath)
            newpath = self.normalize_path(newpath)
            self.client.rename(oldpath, newpath)
        except Exception as e:
            print(e)
    
    def get_files(self, path):
        path = self.normalize_path(path)
        files = []
        for f in self.client.list(path):
            status = self.client.status(join(path, f), False)
            if status['type'] != "DIRECTORY":
                files.append(f)
        return files
    
    def get_folders(self, path):
        path = self.normalize_path(path)
        folders = []
        for f in self.client.list(path):
            status = self.client.status(join(path, f), False)
            if status['type'] == "DIRECTORY":
                folders.append(f)
        return folders
    
    def exists(self, path):
        path = self.normalize_path(path)
        status = self.client.status(path, False)
        return not (status is None)
        
    def isdir(self, path):
        path = self.normalize_path(path)
        status = self.client.status(path, False)
        return status['type'] == "DIRECTORY"
    
    def isfile(self, path):
        path = self.normalize_path(path)
        status = self.client.status(path, False)
        return status['type'] == "FILE"
        
    def read(self, path):
        path = self.normalize_path(path)
        with self.client.read(path) as reader:
            return reader.read().decode('utf-8')
    
    def write(self, path, content):
        path = self.normalize_path(path)
        self.client.write(path, content)
    
    def make_json(self, path):
        normalized_path = self.normalize_path(path)
        data_json = { 'path': urljoin(self.url, normalized_path), 'text': os.path.basename(path) }
        status = self.client.status(normalized_path, False)

        if status is not None:
            data_json['folder'] = status['type'] == "DIRECTORY"
            if status['type'] == "DIRECTORY":
                data_json['nodes'] = [self.make_json(os.path.join(path, fn)) for fn in self.client.list(normalized_path)]
        #print(json.dumps(data_json))
        return data_json
     
    def save_upload(self, file, fullpath):
        localpath = os.path.join(tempfile.gettempdir(), os.path.basename(fullpath))
        if os.path.isfile(localpath):
            os.remove(localpath)
        try:
            file.save(localpath)
            if isfile(fullpath):
                fullpath = os.path.dirname(fullpath)
            self.client.upload(self.normalize_path(fullpath), localpath, True)
        except:
            pass
        
    def download(self, path):
        path = self.normalize_path(path)
        status = self.client.status(path, False)
        if status is not None and status['type'] == "FILE":
            localpath = os.path.join(tempfile.gettempdir(), os.path.basename(path))
            return self.client.download(path, localpath, True)
        else:
            return None
               
class IOHelper():
    @staticmethod
    def getFileSystem(url):
        try:
            u = urlsplit(url)
            if u.scheme == 'http' or u.scheme == 'https':
                return HadoopFileSystem(url, 'hdfs')
        except:
            pass
        return PosixFileSystem()
    
    @staticmethod
    def get_files(path):
        filesystem = IOHelper.getFileSystem(path)
        return filesystem.get_files(path)
    
    @staticmethod
    def get_folders(path):
        filesystem = IOHelper.getFileSystem(path)
        return filesystem.get_folders(path)
    
    @staticmethod
    def remove(path):
        filesystem = IOHelper.getFileSystem(path)
        filesystem.remove(path)
        
    @staticmethod
    def create_folder(path):
        filesystem = IOHelper.getFileSystem(path)
        return filesystem.create_folder(path)
    
    @staticmethod
    def remove(path):
        filesystem = IOHelper.getFileSystem(path)
        filesystem.remove(path)
    
    @staticmethod
    def rename(oldpath, newpath):
        filesystem = IOHelper.getFileSystem(oldpath)
        filesystem.rename(oldpath, newpath)
        
    @staticmethod
    def read(path):
        filesystem = IOHelper.getFileSystem(path)
        return filesystem.read(path)
    
    @staticmethod
    def normalize_path(path):
        filesystem = IOHelper.getFileSystem(path)
        return filesystem.normalize_path(path)
    
    @staticmethod
    def write(path, content):
        filesystem = IOHelper.getFileSystem(path)
        return filesystem.write(path, content)
    
    @staticmethod
    def unique_fs_name(filesystem, path, prefix, ext):

        make_fn = lambda i: os.path.join(path, '{0}({1}){2}'.format(prefix, i, ext))

        for i in range(1, sys.maxsize):
            uni_fn = make_fn(i)
            if not filesystem.exists(uni_fn):
                return uni_fn

    @staticmethod
    def unique_filename(path, prefix, ext):
        filesystem = IOHelper.getFileSystem(path)
        return IOHelper.unique_fs_name(filesystem, path, prefix, ext)
                        
if __name__ == "__main__":
    print("Hello World")
