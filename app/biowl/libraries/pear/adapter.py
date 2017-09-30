import os
from os import path
from exechelper import func_exec_run

localdir = path.join(path.dirname(path.dirname(path.dirname(__file__))), 'storage')
pear = path.join(path.abspath(path.dirname(__file__)), path.join('bin', 'pear'))

def run_pear(*args):
    forward_fastq = "-f {0}".format(path.join(localdir, args[0]))
    reverse_fastq = "-r {0}".format(path.join(localdir, args[1]))
    
    cmdargs = []
    output_file = path.dirname(forward_fastq)
    if len(args) > 2:
        output_file = path.join(localpath, args[2])
        cmdargs.append("-o {0}".format(output_file))
        
    for arg in args[3:]:
        cmdargs.append(arg)
    
    cmdargs.append(forward_fastq)
    cmdargs.append(reverse_fastq)    
    func_exec_run(pear, *cmdargs)
    
    return output_file
    