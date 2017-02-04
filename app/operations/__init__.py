from __future__ import print_function

from ..models import Workflow, WorkItem
from .wordcount.action import Action
import sys
from .discus_p2irc.imgop import register_images

def execute_workflow(workflow_id):
    workflow = Workflow.query.get(workflow_id)
    for workitem in workflow.workitems:
        if (workitem.operation.id == 3):
           return Action.run(workitem)
        elif (workitem.operation.id == 4):
           return register_images(workitem)
        else:
            pass
         
    
