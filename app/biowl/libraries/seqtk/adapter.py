import os
from os import path
from exechelper import func_exec_stdout

localdir = path.join(path.dirname(path.dirname(path.dirname(__file__))), 'storage')
seqtk = path.join(path.abspath(path.dirname(__file__)), path.join('bin', 'seqtk'))

def run_seqtk(*args):
    input = path.join(localdir, args[0])
    command = args[1]
    output = path.join(localdir, args[2])
    
    cmdargs = [command]
    for arg in args[3:]:
        cmdargs.append(arg)
            
    cmdargs.append(input)

    outdata = func_exec_stdout(seqtk, *cmdargs)
    with open(output, 'wb') as f:
        f.write(outdata)
    return output

def seqtk_fastq_to_fasta(*args):
    cmdargs = [args[0], 'seq -a', args[1]]
    for arg in args[2:]:
        cmdargs.append(arg)
    return run_seqtk(*cmdargs)

def seqtk_extract_sample(*args):
    input = path.join(localdir, args[0])
    output = path.join(localdir, args[1])
    
    cmdargs = ['sample']
    if len(args) > 3:
        cmdargs.append('-s ' + str(args[3]))
    
    cmdargs.append(input)
    cmdargs.append(str(args[2]))

    outdata = func_exec_stdout(seqtk, *cmdargs)
    with open(output, 'wb') as f:
        f.write(outdata)
    return output

def seqtk_trim(*args):
    cmdargs = [args[0], 'trimfq', args[1]]
    if len(args) > 2:
        cmdargs.append('-b ' + str(args[2]))
        
    if len(args) > 3:
        cmdargs.append('-e ' + str(args[3]))
    
    for arg in args[4:]:
        cmdargs.append(arg)
        
    return run_seqtk(*cmdargs)