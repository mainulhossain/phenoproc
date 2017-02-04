from __future__ import print_function  

from p2irc.image_tools import registration
from ..tasks import task_manager
from ...models import Task
from ...util import Utility
from os import path
import sys

def register_images(workitem):
    task_id = Task.create_task(workitem.id)
#    print(Utility.get_fullpath(workitem.inputs), file=sys.stderr)
#    print(str(Utility.get_fullpath(workitem.outputs)) + '/', file=sys.stderr)
#    registration.register_channels(str(Utility.get_fullpath(workitem.inputs)) +  '/', str(Utility.get_fullpath(workitem.outputs)) + '/')
    task_manager.submit_func(task_id, registration.register_channels, str(Utility.get_fullpath(workitem.inputs)) +  '/', str(Utility.get_fullpath(workitem.outputs)) + '/')
