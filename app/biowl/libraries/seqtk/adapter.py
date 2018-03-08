import os
from os import path
from ...exechelper import func_exec_stdout
from ...fileop import PosixFileSystem
from ....util import Utility

seqtk = path.join(path.abspath(path.dirname(__file__)), path.join('bin', 'seqtk'))

def run_seqtk(*args, **kwargs):
    input = Utility.get_normalized_path(args[0])
    output = Utility.get_normalized_path(args[2])
    
    cmdargs = [args[1]]
    for arg in args[3:]:
        cmdargs.append(arg)
            
    cmdargs.append(input)

    outdata,_ = func_exec_stdout(seqtk, *cmdargs)
    with open(output, 'wb') as f:
        f.write(outdata)
        
    fs = PosixFileSystem(Utility.get_rootdir(2))
    return fs.strip_root(output)

def seqtk_fastq_to_fasta(*args, **kwargs):
    paramindex = 0
    if 'data' in kwargs.keys():
        data = kwargs['data']
    else:
        if len(args) == paramindex:
            raise ValueError("Argument not given.")
        data = args[paramindex]
        paramindex += 1
                
    if 'output' in kwargs.keys():
        output = kwargs['output']
    else:
        if len(args) > paramindex:
            output = args[paramindex]
            paramindex += 1

    cmdargs = [input, 'seq -a', output]
    for arg in args[paramindex + 1:]:
        cmdargs.append(arg)
    return run_seqtk(*cmdargs)

def seqtk_extract_sample(*args, **kwargs):
    paramindex = 0
    if 'data' in kwargs.keys():
        data = kwargs['data']
    else:
        if len(args) == paramindex:
            raise ValueError("Argument not given.")
        data = args[paramindex]
        paramindex += 1
                
    if 'output' in kwargs.keys():
        output = kwargs['output']
    else:
        if len(args) > paramindex:
            output = args[paramindex]
            paramindex += 1
    
    if 'sample' in kwargs.keys():
        sample = kwargs['sample']
    else:
        if len(args) > paramindex:
            sample = args[paramindex]
            paramindex += 1
        else:
            sample = 10
            
    cmdargs = ['sample -s{0}'.format(sample)]
    
    data = Utility.get_normalized_path(data)
    output = Utility.get_normalized_path(output)
    
    cmdargs.append(data)
    
    outdata,_ = func_exec_stdout(seqtk, *cmdargs)
    with open(output, 'wb') as f:
        f.write(outdata)
        
    fs = PosixFileSystem(Utility.get_rootdir(2))
    return fs.strip_root(output)

def seqtk_trim(*args, **kwargs):
    paramindex = 0
    if 'data' in kwargs.keys():
        data = kwargs['data']
    else:
        if len(args) == paramindex:
            raise ValueError("Argument not given.")
        data = args[paramindex]
        paramindex += 1
                
    if 'output' in kwargs.keys():
        output = kwargs['output']
    else:
        if len(args) > paramindex:
            output = args[paramindex]
            paramindex += 1
    
    if 'begin' in kwargs.keys():
        begin = kwargs['begin']
    else:
        if len(args) > paramindex:
            begin = args[paramindex]
            paramindex += 1
    
    if 'end' in kwargs.keys():
        end = kwargs['end']
    else:
        if len(args) > paramindex:
            end = args[paramindex]
            paramindex += 1
            
    cmdargs = [data, 'trimfq', output]
    if begin:
        cmdargs.append('-b ' + str(begin))
        
    if end:
        cmdargs.append('-e ' + str(end))
    
    for arg in args[4:]:
        cmdargs.append(arg)
        
    return run_seqtk(*cmdargs)