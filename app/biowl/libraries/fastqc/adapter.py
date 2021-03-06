import os
from os import path
from pathlib import Path

from ...exechelper import func_exec_run
from ...fileop import PosixFileSystem
from ....util import Utility

fastqc = path.join(path.abspath(path.dirname(__file__)), path.join('lib', 'fastqc'))

def run_fastqc(*args, **kwargs):
    
    paramindex = 0
    if 'data' in kwargs.keys():
        data = kwargs['data']
    else:
        if len(args) == paramindex:
            raise ValueError("Argument missing error in FastQC.")
        data = args[paramindex]
        paramindex +=1
    
    data = Utility.get_normalized_path(data)
    
    if 'outdir' in kwargs.keys():
        outdir = kwargs['outdir']
    else:
        if len(args) > paramindex:
            outdir = args[paramindex]
            paramindex +=1
    
    if outdir:
        outdir = Utility.get_normalized_path(outdir)
    else:
        outdir = path.dirname(data)
    
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    
    cmdargs = [data, "--outdir=" + outdir]
                       
    for arg in args[2:]:
        cmdargs.append(arg)
    
    outpath = Path(data).stem + "_fastqc.html"
    outpath = os.path.join(outdir, os.path.basename(outpath))
    if os.path.exists(outpath):
        os.remove(outpath)
    
    _,err = func_exec_run(fastqc, *cmdargs)
        
    fs = PosixFileSystem(Utility.get_rootdir(2))
    stripped_path = fs.strip_root(outpath)
    if not os.path.exists(outpath):
        raise ValueError("FastQC could not generate the file " + stripped_path + " due to error " + err)
    
    return stripped_path
    