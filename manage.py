#!/usr/bin/env python
import os

from flask_restful import Api, Resource, reqparse, fields, marshal
from flask_restful.utils import cors
from flask_httpauth import HTTPBasicAuth
from flask import jsonify, request

COV = None
if os.environ.get('FLASK_COVERAGE'):
    import coverage
    COV = coverage.coverage(branch=True, include='app/*')
    COV.start()

if os.path.exists('.env'):
    print('Importing environment from .env...')
    for line in open('.env'):
        var = line.strip().split('=')
        if len(var) == 2:
            os.environ[var[0]] = var[1]

from app import create_app, db
from app.models import User, Follow, Role, Permission, Post, Comment
from flask_script import Manager, Shell
from flask_migrate import Migrate, MigrateCommand
from flask_login import login_required, login_user, logout_user, current_user
from app.models import User
from app.main.views import load_data_sources_biowl, run_biowl, get_user_status, get_task_status

app = create_app(os.getenv('FLASK_CONFIG') or 'default')
manager = Manager(app)
migrate = Migrate(app, db)

api = Api(app)
api.decorators=[cors.crossdomain(origin='*')]
auth = HTTPBasicAuth()

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response


@auth.verify_password
def verify_password(username, password):
    user = User.query.filter_by(email=username).first()
    if user is not None and user.verify_password(password):
        login_user(user)
        return True
    return False

@app.route('/api/datasources')
@auth.login_required
def get_datasources():
    return jsonify({'datasources': load_data_sources_biowl() })

@app.route('/api/script', methods=['POST'])
@auth.login_required
def run_rest_script():
    try:
        script = request.json.get('script')
        args = request.json.get('args') if request.json.get('args') else ''
        immediate = request.json.get('immediate') == 'true'.lower() if request.json.get('immediate') else False
        return run_biowl(current_user.id, script, args, immediate)
    finally:
        logout_user()

@app.route('/api/status/<string:task_id>', methods=['GET'])
@app.route('/api/status/', defaults={'task_id': ''}, methods=['GET'])
@auth.login_required
def get_status_api(task_id):
    if task_id:
        return get_task_status(task_id)
    else:
        return get_user_status(current_user.id)

def make_shell_context():
    return dict(app=app, db=db, User=User, Follow=Follow, Role=Role,
                Permission=Permission, Post=Post, Comment=Comment)
manager.add_command("shell", Shell(make_context=make_shell_context))
manager.add_command('db', MigrateCommand)


@manager.command
def test(coverage=False):
    """Run the unit tests."""
    if coverage and not os.environ.get('FLASK_COVERAGE'):
        import sys
        os.environ['FLASK_COVERAGE'] = '1'
        os.execvp(sys.executable, [sys.executable] + sys.argv)
    import unittest
    tests = unittest.TestLoader().discover('tests')
    unittest.TextTestRunner(verbosity=2).run(tests)
    if COV:
        COV.stop()
        COV.save()
        print('Coverage Summary:')
        COV.report()
        basedir = os.path.abspath(os.path.dirname(__file__))
        covdir = os.path.join(basedir, 'tmp/coverage')
        COV.html_report(directory=covdir)
        print('HTML version: file://%s/index.html' % covdir)
        COV.erase()


@manager.command
def profile(length=25, profile_dir=None):
    """Start the application under the code profiler."""
    from werkzeug.contrib.profiler import ProfilerMiddleware
    app.wsgi_app = ProfilerMiddleware(app.wsgi_app, restrictions=[length],
                                      profile_dir=profile_dir)
    app.run()


@manager.command
def deploy():
    """Run deployment tasks."""
    from flask_migrate import upgrade
    from app.models import Role, User

    # migrate database to latest revision
    upgrade()

    # create user roles
    Role.insert_roles()

    # create self-follows for all users
    User.add_self_follows()


if __name__ == '__main__':
    manager.run()
