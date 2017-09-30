import os
from os import path
from exechelper import func_exec_run

localdir = path.join(path.dirname(path.dirname(path.dirname(__file__))), 'storage')
usearch = path.join(path.abspath(path.dirname(__file__)), path.join('bin', 'usearch10.0.240_i86linux32'))

def run_usearch(*args):
    cmdargs = ["-" + args[0]]
    cmdargs.append(path.join(localdir, args[1]))
    
    output_file = ''
    if len(args) > 2:
        out_opt = args[2]
        cmdargs.append('-' + out_opt)
        output_file = path.join(localdir, args[3])
        cmdargs.append(output_file)

    for arg in args[4:]:
        cmdargs.append('-' + arg)
    
    func_exec_run(usearch, *cmdargs)
    
    return output_file
    