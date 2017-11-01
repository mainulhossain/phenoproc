import os
from os import path
from ...exechelper import func_exec_run
from ...fileop import PosixFileSystem
from ....util import Utility

pear = path.join(path.abspath(path.dirname(__file__)), path.join('bin', 'pear'))

def run_pear(*args):
    
    fs = PosixFileSystem(Utility.get_rootdir(2))
    input1 = fs.normalize_path(Utility.get_quota_path(args[0]))
    input2 = fs.normalize_path(Utility.get_quota_path(args[1]))
    
    forward_fastq = "-f {0}".format(input1)
    reverse_fastq = "-r {0}".format(input2)
    
    cmdargs = []
    output_file = fs.normalize_path(Utility.get_quota_path(args[2]))
    if len(args) > 2:
        output_file = fs.normalize_path(Utility.get_quota_path(args[2]))
        cmdargs.append("-o {0}".format(output_file))
    else:
        raise "Invalid call format for PEAR."
    
    for arg in args[3:]:
        cmdargs.append(arg)
    
    cmdargs.append(forward_fastq)
    cmdargs.append(reverse_fastq)    
    func_exec_run(pear, *cmdargs)
    
    if not os.path.exists(output_file):
        raise "Pear could not generate the file " + fs.strip_root(output_file)
    
    return fs.strip_root(output_file)