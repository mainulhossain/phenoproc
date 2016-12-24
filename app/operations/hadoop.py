import sys
import os
from subprocess import call
import uuid
from .spawn import execute
from flask import current_app

def runHadoop(mapper, reducer, input, output):
        hadoopPath = '/usr/bin/hadoop'
        streamPath = '/usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming-2.7.3.2.5.0.0-1245.jar'
#       call([hadoopPath, 'jar "%s"' % streamPath, '-files mapper-is.py, -libjars WholeFileInputFormat.jar,NamedFileOutputFormat.jar -D mapreduce.input.fileinputformat.input.dir.recursive=true -io rawbytes -mapper "mapper-is.py %s" -input "%s" -output "%s" -inputformat WholeFileInputFormat -outputformat NamedFileOutputFormat' % (src, input, output)])
        #v = 'jar ' + streamPath + ' -files ' + mapper + ',' + reducer + '-D mapreduce.input.fileinputformat.input.dir.recursive=true' + ' -mapper ' + mapper + ' -reducer ' + reducer + -input  -output output'
        jobid = str(uuid.uuid4())
        v = 'jar {0} -files {1},{2} -D mapreduce.input.fileinputformat.input.dir.recursive=true|mapred.job.name="{3}" -mapper {4} -reducer {5} -input {6} -output {7}'.format(streamPath, mapper, reducer, jobid, os.path.basename(mapper), os.path.basename(reducer), input, output)
        print(v, file=sys.stderr)
        #call([hadoopPath, v])
        execute(current_app.config['WEBHDFS_USER'], 'operations', hadoopPath, v)


if __name__ == "__main__":
        runHadoop(sys.argv[1], sys.argv[2], sys.argv[3])
