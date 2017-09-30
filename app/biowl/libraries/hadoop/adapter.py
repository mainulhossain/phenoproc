from __future__ import print_function

import sys
import os
import uuid
from tasks import task_manager

def run_hadoop(mapper, reducer, input, output, **kwargs):
        # Usage: hadoop [--config confdir] [--loglevel loglevel] [COMMAND] [GENERIC_OPTIONS] [COMMAND_OPTIONS]
        # COMMAND: --jar path
        # GENERIC_OPTIONS -files -libjars -D
        # COMMAND_OPTIONS -mapper -reducer -input -output
        hadoopPath = 'echo sr-hadoop | sudo -u hdfs --stdin /usr/bin/hadoop'
        #streamPath = '/usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming-2.7.3.2.5.0.0-1245.jar'
        streamPath = '/usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming.jar'
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
                              
        args = 'jar {0} -files {1},{2} {3} -D mapreduce.input.fileinputformat.input.dir.recursive=true -D mapreduce.job.name="{4}" {5} -mapper "{6}" -reducer {7} -input {8} -output {9}'.format(streamPath, mapper, reducer, generic_options, jobid, command_options, mapper_arg, os.path.basename(reducer), input, output)
        print(hadoopPath + " " + args, file=sys.stderr)
        task_manager.submit([hadoopPath, args])

def run_hadoop_example(program, input, output, expr):
        hadoopPath = 'echo sr-hadoop | sudo -u hdfs --stdin /usr/bin/hadoop'
        examplePath = '/usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-mapreduce-examples.jar'
        args = 'jar {0} {1} {2} {3} {4}'.format(examplePath, program, input, output, expr)
        print(hadoopPath + " " + args, file=sys.stderr)
        task_manager.submit([hadoopPath, args])

def count_words(input, output):
    cwd = os.path.abspath(os.path.dirname(__file__))
    run_hadoop(os.path.join(cwd, 'wordcount/mapper.py'), os.path.join(cwd, 'wordcount/reducer.py'), input, output)
            
def search_word(input, output, expr):
    run_hadoop_example('grep', input, output, expr)
    
if __name__ == "__main__":
    run_hadoop(sys.argv[1], sys.argv[2])
