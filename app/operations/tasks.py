from __future__ import print_function

from concurrent.futures import ThreadPoolExecutor
from subprocess import call, check_output
from past.builtins.misc import execfile
from ..models import Task, TaskStatus, TaskLog
from .. import db
from sqlalchemy import func

import sys

class TaskManager:
    def __init__(self, max_count = 5):
        self.pool = ThreadPoolExecutor(max_count)
        
    def submit(self, task_id, argv):
        execfile = argv[:1]
        args = argv[1:]
        #self.futures = {task_id: self.pool.submit(check_output, argv, shell=True)}
        print(' '.join(argv))
        self.futures = {task_id: self.pool.submit(check_output, ' '.join(argv), shell=True)}
        task = Task.query.get(task_id)        
        task.add_log(TaskStatus.query.get(3))
    
    def check_task(self, task_id):
        status = self.manage_running_task(task_id)
        if status is None:
            logs = db.session.query(TaskLog.status, func.max(TaskLog.time)).filter(TaskLog.task_id==task_id).group_by(TaskLog.task_id)
            if logs:
                status = logs.first().status
        return status
    
    def manage_running_task(self, task_id):
        status = self.managed_task_status(task_id)
        if status is not None:
            Task.query.get(task_id).add_log(status)
            if status.id != 3:
                del self.futures[task_id]
        return status;         
    
    def managed_tasks_to_tasklogs(self):
        for t in self.futures:
            self.check_running_task(t)
            
    def managed_task_status(self, task_id):
        future = self.futures[task_id]
        status = TaskStatus.query.get(0)
        if future is not None:
            if future.cancelled():
                status = TaskStatus.query.get(6)
            elif future.running:
                status = TaskStatus.query.get(3)
            elif future.done():
                status = TaskStatus.query.get(4)
                #output = future.result()
        return status
            
task_manager = TaskManager()