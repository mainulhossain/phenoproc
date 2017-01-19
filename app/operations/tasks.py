from __future__ import print_function

from concurrent.futures import ThreadPoolExecutor
from subprocess import call, check_output
from past.builtins.misc import execfile
from ..models import Task, TaskStatus, TaskLog
from .. import db
from sqlalchemy import func
from enum import Enum
import sys

class TaskStatusTypes(Enum):
    Unknown = 1
    Created = 2
    Running = 3
    Completed = 4
    Faulted = 5
    Cancelling = 6
    Canceled = 7
    
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
        task.add_log(TaskStatus.query.get(TaskStatusTypes.Running)) # 3 means Running
    
    def check_task(self, task_id):
        status = self.manage_running_task(task_id)
        if status is None:
            status = self.manage_db_task(task_id)
        return status
    
    def get_running_logs(self):
        return db.session.query(TaskLog.status, func.max(TaskLog.time)).group_by(TaskLog.task_id).having(TaskLog.status_id == TaskStatusTypes.Running)
        
    def manage_db_task(self, task_id):
        logs = db.session.query(TaskLog.status, func.max(TaskLog.time)).filter(TaskLog.task_id==task_id).group_by(TaskLog.task_id)
        if logs:
            return logs.first().status
        return None
    
    def cancel_task(self, task_id):
        future = self.futures[task_id]
        if future is not None:
            del self.futures[task_id] # remove orphan task
            Task.query.get(task_id).add_log(TaskStatus.query.get(TaskStatusTypes.Cancelling))
            future.cancel()
            Task.query.get(task_id).add_log(TaskStatus.query.get(TaskStatusTypes.Cancelled))
        else:
            Task.query.get(task_id).add_log(TaskStatus.query.get(TaskStatusTypes.Cancelled))
                    
    def manage_running_task(self, task_id):
        status = self.managed_task_status(task_id)
        if status is not None:
            Task.query.get(task_id).add_log(status)
            if status.id != TaskStatusTypes.Running:
                del self.futures[task_id] # remove orphan task
        return status;         
    
    def managed_tasks_to_tasklogs(self):
        for t in self.futures:
            self.check_running_task(t)
            
    def managed_task_status(self, task_id):
        future = self.futures[task_id]
        status = TaskStatus.query.get(TaskStatusTypes.Unknown)
        if future is not None:
            if future.cancelled():
                status = TaskStatus.query.get(TaskStatusTypes.Cancelled)
            elif future.running:
                status = TaskStatus.query.get(TaskStatusTypes.Running)
            elif future.done():
                status = TaskStatus.query.get(TaskStatusTypes.Completed)
                #output = future.result()
        return status
            
task_manager = TaskManager()