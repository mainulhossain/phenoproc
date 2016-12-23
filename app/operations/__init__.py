from ..models import Workflow, WorkItem
from .wordcount.action import Action

def execute_workflow(workflow_id):
#    raise ValueError("Error in execution")
     workflow = Workflow.query.get(workflow_id)
     for workitem in workflow.workitems:
         if (workitem.operation.name == 'wordcount'):
            return Action.run(workitem)
         else:
             pass
         
    