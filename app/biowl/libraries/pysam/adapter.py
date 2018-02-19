import os
import pysam
from os import path
from ...fileop import PosixFileSystem
from ....util import Utility

def run_sam_to_bam(*args, **kwargs):
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
    
    if output:
        output = Utility.get_normalized_path(output)
    else:
        output = data + ".bam"
        output = Utility.get_normalized_path(output)
    
    infile = pysam.AlignmentFile(data, "r")
    outfile = pysam.AlignmentFile(output, "wb")
    for s in infile:
        outfile.write(s)
    
    fs = PosixFileSystem(Utility.get_rootdir(2))
    if not os.path.exists(output):
        raise ValueError("bwa could not generate the file " + fs.strip_root(output))
    
    return fs.strip_root(output)