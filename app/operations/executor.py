from __future__ import print_function

from ..models import Task, TaskStatus, TaskLog, Workflow, WorkItem
from concurrent.futures import ThreadPoolExecutor
#from .discus_p2irc.imgop import register_images
from .image_search.imgproc import search_image
from .wordcount.action import Action
import time
from .tasks import task_manager
import sys
#from manage import app

class WorkflowExecutor:
    def __init__(self, max_count = 8):
        self.pool = ThreadPoolExecutor(max_count)
        self.futures = dict()
        
    @staticmethod
    def is_workitem_running(workitem_id):
        tasks = Task.query.filter_by(workitem_id = workitem_id)
        for t in tasks:
            if task_manager.is_running(t.id):
                return True
        return False
    
    @staticmethod
    def is_running(workflow_id):
        workflow = Workflow.query.get(workflow_id)
        if workflow is None:
            return False
        for w in workflow.workitems:
            if WorkflowExecutor.is_workitem_running(w.id):
                return True
        return False
    
    @staticmethod
    def cancel(workflow_id):
        workitems = WorkItem.query.filter_by(workflow_id = workflow_id)
        for w in workitems:
            tasks = Task.query.filter_by(workitem_id = w.id)
            for t in tasks:
                if task_manager.is_running(t.id):
                    task_manager.cancel_task(t.id)
    
    @staticmethod
    def submit(workflow_id):
        with app.app_context():
            workitems = WorkItem.query.filter_by(workflow_id = workflow_id)
            for workitem in workitems:
                if (workitem.operation.id == 3):
                   Action.run(workitem)
                elif (workitem.operation.id == 4):
                   register_images(workitem)
                elif (workitem.operation.id == 5):
                    search_image(workitem)
                else:
                    pass
                while WorkflowExecutor.is_workitem_running(workitem.id):
                    time.sleep(3)
                    
    def run(self, workflow_id):
        workflow = Workflow.query.get(workflow_id)
        if workflow is None:
            return False
        
        if WorkflowExecutor.is_running(workflow_id):
            WorkflowExecutor.cancel(workflow_id)
        
        self.futures = { workflow_id: self.pool.submit(WorkflowExecutor.submit, workflow_id)}
        
workflow_executor = WorkflowExecutor()