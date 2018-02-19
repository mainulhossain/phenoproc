import os
from os import path
from ...exechelper import func_exec_run
from ...fileop import PosixFileSystem
from ....util import Utility

bwa = path.join(path.abspath(path.dirname(__file__)), path.join('bin', 'bwa'))
   
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
        output = data1 + ".sam"
        output = Utility.get_normalized_path(output)
        
    cmdargs = [ref, data1]
    if data2:
        cmdargs.append(data2)
        
    cmdargs.append("-o {0}".format(output))
    
    for arg in args[paramindex + 1:]:
        cmdargs.append(arg)
    
    func_exec_run(bwa, *cmdargs)
    
    fs = PosixFileSystem(Utility.get_rootdir(2))
    if not os.path.exists(output):
        raise ValueError("bwa could not generate the file " + fs.strip_root(output))
    
    return fs.strip_root(output)