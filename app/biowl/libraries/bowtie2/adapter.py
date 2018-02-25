import os
from os import path
from pathlib import Path

from ...exechelper import func_exec_run
from ...fileop import PosixFileSystem
from ....util import Utility

bowtie2 = path.join(path.abspath(path.dirname(__file__)), path.join('bin', 'bowtie2'))
bowtie2_build = path.join(path.abspath(path.dirname(__file__)), path.join('bin', 'bowtie2-build'))

def build_bwa_index(ref):
    cmdargs = [ref, ref]
    return func_exec_run(bowtie2_build, *cmdargs)
    
def run_bwa(*args, **kwargs):
    
    paramindex = 0
    if 'ref' in kwargs.keys():
        ref = kwargs['ref']
    else:
        if len(args) == paramindex:
            raise ValueError("Argument error")
        ref = args[paramindex]
        paramindex +=1
        
    ref = Utility.get_normalized_path(ref)
    
    indexpath = Path(ref).stem + ".bwt"
    indexpath = os.path.join(os.path.dirname(ref), os.path.basename(indexpath))
    if not os.path.exists(indexpath):
        build_bwa_index(ref)
    
    if 'data1' in kwargs.keys():
        data1 = kwargs['data1']
    else:
        if len(args) == paramindex:
            raise ValueError("Argument error")
        data1 = args[paramindex]
        paramindex +=1
    
    data1 = Utility.get_normalized_path(data1)
    
    if 'data2' in kwargs.keys():
        data2 = kwargs['data2']
    else:
        if len(args) > paramindex:
            data2 = args[paramindex]
            paramindex +=1
    
    if data2:
        data2 = Utility.get_normalized_path(data2)
        
    if 'output' in kwargs.keys():
        output = kwargs['output']
    else:
        if len(args) > paramindex:
            output = args[paramindex]
            paramindex +=1
    
    if output:
        output = Utility.get_normalized_path(output)
    else:
        output = Path(data1).stem + ".sam"
        output = os.path.join(os.path.dirname(data1), os.path.basename(output))
        output = Utility.get_normalized_path(output)
    
    if not os.path.exists(path.dirname(output)):
        os.makedirs(path.dirname(output))
        
    if os.path.exists(output):
        os.remove(output)
            
    cmdargs = ['-q -p 4 -x', ref]
    if data2:
        cmdargs.extend("-1 {0}".format(data1), "-2 {0}".format(data2))
    else:
        cmdargs.extend("-U {0}".format(data1))
        
    cmdargs.append("-S {0}".format(output))
    
    for arg in args[paramindex + 1:]:
        cmdargs.append(arg)
    
    _,err = func_exec_run(bwa, *cmdargs)
    
    fs = PosixFileSystem(Utility.get_rootdir(2))
    if not os.path.exists(output):
        raise ValueError("bowtie2 could not generate the file " + fs.strip_root(output) + " due to error " + err)
    
    return fs.strip_root(output)