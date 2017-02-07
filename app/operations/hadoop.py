from __future__ import print_function

import sys
import os
from subprocess import call
import uuid
from . import spawn
from flask import current_app
from .tasks import task_manager

def runHadoop(task_id, mapper, reducer, input, output, **kwargs):
        # Usage: hadoop [--config confdir] [--loglevel loglevel] [COMMAND] [GENERIC_OPTIONS] [COMMAND_OPTIONS]
        # COMMAND: --jar path
        # GENERIC_OPTIONS -files -libjars -D
        # COMMAND_OPTIONS -mapper -reducer -input -output
        hadoopPath = '/usr/bin/hadoop'
        streamPath = '/usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming-2.7.3.2.5.0.0-1245.jar'
        jobid = str(uuid.uuid4())
        
        generic_options = ''
        if 'generic_options' in kwargs:
            generic_options = kwargs['generic_options']
        command_options = ''
        if 'command_options' in kwargs:
            command_options = kwargs['command_options']
            
        mapper_arg = os.path.basename(mapper)
        if 'mapper_arg' in kwargs:
            mapper_arg = mapper_arg + " " + kwargs['mapper_arg']
                
#         if 'inputformat' in kwargs:
#             genericOptions = ' -libjars{0}'.format(kwargs['inputformat'])
#             commandOptions = ' -inputformat{0}'.format(kwargs['inputformat'])
#         if 'outputformat' in kwargs:
#             if not genericOptions:
#                 genericOptions = ' -libjars{0}'.format(kwargs['outputformat'])
#             else:
#                 genericOptions = genericOptions + ',' + kwargs['outputformat']
#                 
#             commandOptions = commandOptions + ' -outputformat{0}'.format(kwargs['outputformat'])
#         
#         if 'io' in kwargs:
#             commandOptions = commandOptions + ' -io{0}'.format(kwargs['io'])
                
        args = 'jar {0} -files {1},{2} {3} -D mapreduce.input.fileinputformat.input.dir.recursive=true -D mapreduce.job.name="{4}" {5} -mapper "{6}" -reducer {7} -input {8} -output {9}'.format(streamPath, mapper, reducer, generic_options, jobid, command_options, mapper_arg, os.path.basename(reducer), input, output)
        print(args, file=sys.stderr)
        task_manager.submit(task_id, [hadoopPath, args])

if __name__ == "__main__":
        runHadoop(sys.argv[1], sys.argv[2], sys.argv[3])
