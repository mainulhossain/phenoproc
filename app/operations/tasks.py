from __future__ import print_function

from concurrent.futures import ThreadPoolExecutor
from subprocess import call
from past.builtins.misc import execfile
from ..models import Task, TaskStatus

import sys

class TaskManager:
    def __init__(self, max_count = 5):
        self.pool = ThreadPoolExecutor(max_count)
        
    def submit(self, task_id, argv):
        execfile = argv[:1]
        args = argv[1:]
        self.futures = {'task_id': task_id, 'future': self.pool.submit(call, [execfile, args])}
        task = Task.query.get(task_id)        
        task.add_log(TaskStatus.Running)
    
    def check_task(self, task_id):
        future = self.futures[task_id]
        status = TaskStatus.Unknown
        if future is not None:
            if future.cancelled():
                status = TaskStatus.Cancelled
            elif future.running:
                status = TaskStatus.Running
            elif future.done():
                status = TaskStatus.Compeleted
        if status is not None:
            Task.query.get(task_id).add_log(status)
        return status;         
    
task_manager = TaskManager()