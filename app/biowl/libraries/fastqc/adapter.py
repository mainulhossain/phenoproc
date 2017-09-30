import os
from os import path
from exechelper import func_exec_run
from fileop import PosixFileSystem

fastqc = path.join(path.abspath(path.dirname(__file__)), path.join('lib', 'fastqc'))

def run_fastqc(*args):
    fs = PosixFileSystem()
    input = fs.normalize_path(args[0])
    cmdargs = [input]
    outdir = path.dirname(input)
    if len(args) > 1:
        outdir = fs.normalize_path(args[1])
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        cmdargs.append("--outdir=" + outdir)
        
    for arg in args[2:]:
        cmdargs.append(arg)
    
    func_exec_run(fastqc, *cmdargs)
    outname = path.basename(input)
    outname = outname.split(os.extsep)[0] + "_fastqc.html"
    
    outpath = path.join(outdir, outname)
    return fs.strip_root(outpath)
    