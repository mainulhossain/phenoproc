from celery.contrib.abortable import AbortableTask, AbortableAsyncResult
from celery.states import state, PENDING, SUCCESS

from flask_login import current_user

from datetime import datetime
import os
import random
import time
import json

from config import Config
from . import celery, create_app
from .biowl.dsl.parser import PhenoWLParser, PythonGrammar
from .biowl.timer import Timer
from .models import Runnable

class ContextTask(AbortableTask):
    abstract = True
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print('{0!r} failed: {1!r}'.format(task_id, exc)) # log to runnables by task_id
    def __call__(self, *args, **kwargs):
        app = create_app(os.getenv('FLASK_CONFIG') or 'default')
        app.config['CURRENT_USER'] = 'phenoproc' #current_user.username
        with app.app_context():
            return AbortableTask.__call__(self, *args, **kwargs)

class RequestContextTask(AbortableTask):
    """Celery task running within Flask test request context.
    Expects the associated Flask application to be set on the bound
    Celery application.
    """
    abstract = True
    def __call__(self, *args, **kwargs):
        """Execute task."""
        with self.app.flask_app.test_request_context():
            self.app.flask_app.try_trigger_before_first_request_functions()
            self.app.flask_app.preprocess_request()
            res = AbortableTask.__call__(self, *args, **kwargs)
            self.app.flask_app.process_response(
                self.app.flask_app.response_class())
            return res
        
def long_task(self):
    """Background task that runs a long function with progress reports."""
    verb = ['Starting up', 'Booting', 'Repairing', 'Loading', 'Checking']
    adjective = ['master', 'radiant', 'silent', 'harmonic', 'fast']
    noun = ['solar array', 'particle reshaper', 'cosmic ray', 'orbiter', 'bit']
    message = ''
    total = random.randint(10, 50)
    for i in range(total):
        if not message or random.random() < 0.25:
            message = '{0} {1} {2}...'.format(random.choice(verb),
                                              random.choice(adjective),
                                              random.choice(noun))
        self.update_state(state='PROGRESS',
                          meta={'current': i, 'total': total,
                                'status': message})
        time.sleep(1)
    return {'current': 100, 'total': 100, 'status': 'Task completed!', 'result': 42}

@celery.task(bind=True, base=ContextTask)#, base = AbortableTask
def run_script(self, machine, script, args):
    parserdir = Config.BIOWL
    curdir = os.getcwd()
    os.chdir(parserdir) #set dir of this file to current directory
    duration = 0
    try:
        machine.context.reload()
        parser = PhenoWLParser(PythonGrammar())   
        with Timer() as t:
            if args:
                args_tokens = parser.parse_subgrammar(parser.grammar.arguments, args)
                machine.args_to_symtab(args_tokens) 
            prog = parser.parse(script)
            machine.run(prog)
        duration = float("{0:.3f}".format(t.secs))
    except:
        machine.context.err.append("Error in parse and interpretation")
    finally:
        os.chdir(curdir)
#    return { 'out': machine.context.out, 'err': machine.context.err, 'duration': "{:.4f}s".format(duration) }
    return { 'out': machine.context.out, 'err': machine.context.err, 'duration': duration }

def stop_script(task_id):
#     abortable_task = AbortableAsyncResult(task_id)
#     abortable_task.abort()
    from celery.task.control import revoke
    revoke(task_id, terminate=True)

def sync_task_status_with_db(task):
    status = None
    
    if task.celery_id is not None and task.status != 'FAILURE' and task.status != 'SUCCESS' and task.status != 'REVOKED':
        celeryTask = run_script.AsyncResult(task.celery_id)
        task.status = celeryTask.state
        
        if celeryTask.state != 'PENDING':
            if celeryTask.state != 'FAILURE':
                task.out = "\n".join(celeryTask.info.get('out'))
                task.err = "\n".join(celeryTask.info.get('err'))
                task.duration = int(celeryTask.info.get('duration'))
            else:
                task.err = str(celeryTask.info)
        task.update()

    return task.status
    
def sync_task_status_with_db_for_user(user_id):
        tasks = Runnable.query.filter(Runnable.user_id == user_id)
        for task in tasks:
            sync_task_status_with_db(task)