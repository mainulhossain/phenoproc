from __future__ import print_function

from ...models import Workflow, WorkItem, DataSource, Task
import sys
from ..hadoop import runHadoop
import os
from ..tasks import task_manager
from ...util import Utility
from flask_login import current_user

#wordcount = Blueprint('wordcount', __name__)
def search_image(workitem): 
    input_datasource = DataSource.query.get(workitem.inputs.datasource_id)
    output_datasource = DataSource.query.get(workitem.outputs.datasource_id)
    task_id = Task.create_task(workitem.id)
    cwd = os.path.abspath(os.path.dirname(__file__))
    
    options1 = "-libjars {0}{1}".format(os.path.join(cwd, 'WholeFileInputFormat.jar'), os.path.join(cwd, 'NamedFileOutputFormat.jar'))
    options2 = ' -io rawbytes'
    options3 = os.path.join(Utility.get_rootdir(workitem.inputs.datasource_id), current_user.username, 'image_src/frame00115.jpg')
    runHadoop(task_id, os.path.join(cwd, 'mapper.py'), os.path.join(cwd, 'reducer.py'), Utility.get_fullpath(workitem.inputs), Utility.get_fullpath(workitem.outputs), generic_options = options1, command_options = options2, mapper_arg = options3)