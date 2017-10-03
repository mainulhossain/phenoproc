from __future__ import print_function

from concurrent.futures import ThreadPoolExecutor
from subprocess import call, check_output
from past.builtins.misc import execfile
import sys
from ..models import Runnable, TaskStatus
from ..operations.tasks import TaskStatusTypes

class TaskManager:
    def __init__(self, max_count = 5):
        self.pool = ThreadPoolExecutor(max_count)
        self.futures = []
        
    def submit_func(self, func, *args):
        self.cleanup_pool()
        future = self.pool.submit(func, *args)
        self.futures.append(future)
    
    def submit(self, argv):
        self.cleanup_pool()
        execfile = argv[:1]
        args = argv[1:]
        future = self.pool.submit(check_output, ' '.join(argv), shell=True)
        self.futures.append(future)
        return future.result()
        
    def cleanup_pool(self):
        self.futures = list(filter(lambda f : f and not f.done(), self.futures))
    
    def wait(self):
        for future in self.futures:
            future.result() # blocks and forward exceptions
            
    def idle(self):
        '''
        True if no task is running
        '''
        for future in self.futures:
            if not future.done():
                return False
        return True
                
class RunnableManager:
    def __init__(self, max_count = 5):
        self.pool = ThreadPoolExecutor(max_count)
        self.futures = {}

    def submit_func(self, task_id, func, *args):
        self.futures[task_id] = self.pool.submit(func, *args)
        task = Runnable.query.get(task_id)
        task.update_status(TaskStatus.query.get(int(TaskStatusTypes.Running)))
    
    def submit(self, task_id, argv):
        execfile = argv[:1]
        args = argv[1:]
        #self.futures = {task_id: self.pool.submit(check_output, argv, shell=True)}
        self.futures[task_id] = self.pool.submit(check_output, ' '.join(argv), shell=True)
        task = Runnable.query.get(task_id)        
        task.update_status(TaskStatus.query.get(int(TaskStatusTypes.Running)))
    
    def running_in_pool(self, task_id):
        if not task_id in self.futures:
            return false
        future = self.futures[task_id]
        return future.running()
    
    def sync_all_task_status_with_db(self):
        tasks = Runnable.query.all()
        for task in tasks:
            self.sync_task_status_with_db(task)
    
    def sync_task_status_with_db_for_user(self, user_id):
        tasks = Runnable.query.filter(Runnable.user_id == user_id)
        for task in tasks:
            self.sync_task_status_with_db(task)
                    
    def sync_task_status_with_db(self, task):
        status = None
      
        status = TaskStatus.query.get(int(TaskStatusTypes.Unknown))
        if task.id in self.futures:
            future = self.futures[task.id]
            
            if future.cancelled():
                status = TaskStatus.query.get(int(TaskStatusTypes.Canceled))
                if status.id != task.status_id:
                    task.out = str(future.result())
                    task.err = str(TaskStatusTypes.Canceled)
                    task.status = status
                    task.update()
                del self.futures[task.id]
            elif future.running():
                status = TaskStatus.query.get(int(TaskStatusTypes.Running))
                if status.id != task.status_id:
                    task.status = status
                    task.update()
            elif future.done():
                status = TaskStatus.query.get(int(TaskStatusTypes.Completed))
                if status.id != task.status_id:
                    task.out = str(future.result())
                    task.status = status
                    task.update()
                del self.futures[task.id]
        else:
            if task.status_id == int(TaskStatusTypes.Running):
                task.err = "Task abandoned."
                task.status = status
                task.update()
                
        return task.status
                    
    def cleanup_pool(self):
        self.futures = list(filter(lambda f : f and not f.done(), self.futures))
    
    def wait(self):
        for future in self.futures:
            future.result() # blocks and forward exceptions
            
    def idle(self):
        '''
        True if no task is running
        '''
        for future in self.futures:
            if not future.done():
                return False
        return True
                
task_manager = TaskManager()
                
runnable_manager = RunnableManager()
