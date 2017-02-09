from __future__ import print_function

from concurrent.futures import ThreadPoolExecutor
from subprocess import call, check_output
from past.builtins.misc import execfile
from ..models import Task, TaskStatus, TaskLog, Workflow
from .. import db
from sqlalchemy import func
from enum import Enum,IntEnum
import sys

class TaskStatusTypes(IntEnum):
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
        self.futures = dict()
        
    def submit_func(self, task_id, func, *args):
        self.futures = { task_id: self.pool.submit(func, *args) }
        task = Task.query.get(task_id)        
        task.add_log(TaskStatus.query.get(int(TaskStatusTypes.Running))) # 3 means Running
    
    def submit(self, task_id, argv):
        execfile = argv[:1]
        args = argv[1:]
        #self.futures = {task_id: self.pool.submit(check_output, argv, shell=True)}
        self.futures = {task_id: self.pool.submit(check_output, ' '.join(argv), shell=True)}
        task = Task.query.get(task_id)        
        task.add_log(TaskStatus.query.get(int(TaskStatusTypes.Running))) # 3 means Running
    
    def check_task(self, task_id):
        status = self.manage_running_task(task_id)
        if status is None:
            status = self.manage_db_task(task_id)
        return status
    
    def get_running_logs(self):
        return db.session.query(TaskLog.status, func.max(TaskLog.time)).group_by(TaskLog.task_id).having(TaskLog.status_id == int(TaskStatusTypes.Running))
        
    def manage_db_task(self, task_id):
        logs = db.session.query(TaskLog.status, func.max(TaskLog.time)).filter(TaskLog.task_id==task_id).group_by(TaskLog.task_id)
        if logs:
            return logs.first().status
        return None
    
    def cancel_task(self, task_id):
        if task_id in self.futures:
            future = self.futures[task_id]
            del self.futures[task_id] # remove orphan task
            Task.query.get(task_id).add_log(TaskStatus.query.get(int(TaskStatusTypes.Cancelling)))
            future.cancel()
            Task.query.get(task_id).add_log(TaskStatus.query.get(int(TaskStatusTypes.Canceled)))
        else:
            Task.query.get(task_id).add_log(TaskStatus.query.get(int(TaskStatusTypes.Canceled)))
                    
    def manage_running_task(self, task_id):
        status = self.managed_task_status(task_id)
        if status is not None:
            Task.query.get(task_id).add_log(status)
            if status.id != int(TaskStatusTypes.Running):
                del self.futures[task_id] # remove orphan task
        return status;         
    
    def managed_tasks_to_tasklogs(self):
        for t in self.futures:
            self.check_running_task(t)
            
    def managed_task_status(self, task_id):
        status = None
        if task_id in self.futures:
            future = self.futures[task_id]
            status = TaskStatus.query.get(int(TaskStatusTypes.Unknown))
            if future.cancelled():
                status = TaskStatus.query.get(int(TaskStatusTypes.Canceled))
            elif future.running:
                status = TaskStatus.query.get(int(TaskStatusTypes.Running))
            elif future.done():
                status = TaskStatus.query.get(int(TaskStatusTypes.Completed))
                #output = future.result()
        return status
    
    def is_running(self, task_id):
        status = self.managed_task_status(task_id)
        return status is not None and status.id == int(TaskStatusTypes.Running)
            
task_manager = TaskManager()
