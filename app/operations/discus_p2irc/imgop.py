from p2irc.image_tools import registration
from ..tasks import task_manager
from ...models import Task
from ...util import Utility

def register_images(workitem):
    task_id = Task.create_task(workitem.id)
    task_manager.submit(task_id, registration.register_channels, [Utility.get_fullpath(workitem.inputs), Utility.get_fullpath(workitem.outputs)])