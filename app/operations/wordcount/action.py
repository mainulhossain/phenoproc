from __future__ import print_function

from ...models import Workflow, WorkItem, DataSource, OperationSource, Task
import sys
from subprocess import call
from ..hadoop import runHadoop
import os
from ..tasks import task_manager

#wordcount = Blueprint('wordcount', __name__)
class Action(object):
    @staticmethod
    def run(workitem): 
        operationsource = OperationSource.query.get(workitem.operation.operationsource_id)
        if operationsource.id == 1:
            Action.run_hdfs(workitem)
        elif operationsource.id == 4:
            Action.run_fs(workitem)
            
    @staticmethod
    def run_hdfs(workitem):
        input_datasource = DataSource.query.get(workitem.inputs.datasource_id)
        output_datasource = DataSource.query.get(workitem.outputs.datasource_id)
        task_id = Task.create_task(workitem.id)
        runHadoop(task_id, os.path.abspath('mapper.py'), os.path.abspath('reducer.py'), os.path.abspath(os.path.dirname(__file__)), os.path.join(input_datasource.url, workitem.inputs.get_fullpath()), os.path.join(output_datasource.url, workitem.outputs.get_fullpath()))
    
    @staticmethod
    def run_fs(workitem):
        task_id = Task.create_task(workitem.id)
        task_manager.submit(task_id, ['ls', '-l'])
        
#     @staticmethod
#     def run_fs(workitem):
#         sys.stdin = open('*.txt')
#         import mapper.py
#         outputFile =''
#         with open('outputFile', 'w') as f:
#             call(['python', 'mapper.py'], stdout=f)