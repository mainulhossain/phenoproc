from __future__ import print_function

from ..models import Workflow, WorkItem
from .wordcount.action import Action
import sys

def execute_workflow(workflow_id):
    workflow = Workflow.query.get(workflow_id)
    for workitem in workflow.workitems:
        if (workitem.operation.name == 'Python Word Count'):
           return Action.run(workitem)
        else:
            pass
         
    