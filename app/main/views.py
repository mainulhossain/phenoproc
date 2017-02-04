from __future__ import print_function

from flask import Flask, render_template, redirect, url_for, abort, flash, request,\
    current_app, make_response, g
from flask import send_from_directory
from flask_login import login_required, current_user
from flask_sqlalchemy import get_debug_queries
from . import main
from .forms import EditProfileForm, EditProfileAdminForm, PostForm, CommentForm
from .. import db
from ..models import Permission, Role, User, Post, Comment, Workflow, WorkItem, DataSource, Data, DataType, OperationSource, Operation
from ..decorators import admin_required, permission_required
from sqlalchemy import text
import os
import sys
import flask_sijax
from ..operations import execute_workflow
from .ajax import WorkflowHandler
from ..util import Utility
from ..io import PosixFileSystem, HadoopFileSystem, getFileSystem
import json
from werkzeug import secure_filename

app = Flask(__name__)

@main.after_app_request
def after_request(response):
    for query in get_debug_queries():
        if query.duration >= current_app.config['PHENOPROC_SLOW_DB_QUERY_TIME']:
            current_app.logger.warning(
                'Slow query: %s\nParameters: %s\nDuration: %fs\nContext: %s\n'
                % (query.statement, query.parameters, query.duration,
                   query.context))
    return response


@main.route('/shutdown')
def server_shutdown():
    if not current_app.testing:
        abort(404)
    shutdown = request.environ.get('werkzeug.server.shutdown')
    if not shutdown:
        abort(500)
    shutdown()
    return 'Shutting down...'

def load_data_sources():
        # construct data source tree
    datasources = DataSource.query.all()
    datasource_tree = { 'type': DataType.Custom, 'children': [] }
    for ds in datasources:
        datasource = { 'datasource': ds.id, 'type': DataType.Root, 'base':'', 'path': ds.url, 'name': ds.name, 'children': []}
        if ds.id == 1:
            # hdfs tree
            try:
                hdfs = HadoopFileSystem()
                if current_user.is_authenticated:
                    datasource['children'].append(hdfs.make_json(ds.id, Utility.get_rootdir(ds.id), current_user.username))
                datasource['children'].append(hdfs.make_json(ds.id, Utility.get_rootdir(ds.id), current_app.config['PUBLIC_DIR']))
            except:
                pass

        elif ds.id == 2:
            # file system tree
            posixFS = PosixFileSystem()
            if current_user.is_authenticated:
                datasource['children'].append(posixFS.make_json(ds.id, Utility.get_rootdir(ds.id), current_user.username))
            datasource['children'].append(posixFS.make_json(ds.id, Utility.get_rootdir(ds.id), current_app.config['PUBLIC_DIR']))
 
        datasource_tree['children'].append(datasource)
        
    return datasource_tree

@main.route('/reloaddatasources', methods=['POST'])
def load_data_sources_json():
    return json.dumps(load_data_sources())

@main.route('/', defaults={'id': ''}, methods = ['GET', 'POST'])
@main.route('/workflow/<int:id>/', methods = ['GET', 'POST'])
def index(id=None):
              
    id = Utility.ValueOrNone(id)
    if id <= 0:
        id = request.args.get('workflow')
                                                               
    if g.sijax.is_sijax_request:
        # Sijax request detected - let Sijax handle it
        g.sijax.register_object(WorkflowHandler)
        return g.sijax.process_request()

    form = PostForm()
    if current_user.can(Permission.WRITE_ARTICLES) and form.validate_on_submit():
        post = Post(body=form.body.data, author=current_user._get_current_object())
        db.session.add(post)
        return redirect(url_for('.index'))
    page = request.args.get('page', 1, type=int)
    show_followed = False
    if current_user.is_authenticated:
        show_followed = bool(request.cookies.get('show_followed', ''))
    if show_followed:
        query = current_user.followed_posts
    else:
        query = Post.query
    pagination = query.order_by(Post.timestamp.desc()).paginate(
        page, per_page=current_app.config['PHENOPROC_POSTS_PER_PAGE'],
        error_out=False)
    posts = pagination.items
    
    datasource_tree = load_data_sources()
    
    # construct operation source tree
    operationsources = OperationSource.query.all()
    operation_tree = { 'name' : ('operations', ''), 'children' : [] }
    for ops in operationsources:
        operation_tree['children'].append({ 'name' : (ops.name, ops.id), 'children' : [] })
        for op in ops.operations:
            operation_tree['children'][-1]['children'].append({ 'name' : (op.name, op.id), 'children' : [] })
    
    # workflows tree
    workflows = []
    if current_user.is_authenticated:
        #workflows = Workflow.query.filter_by(user_id=current_user.id)
        #sql = 'SELECT workflows.*, MAX(time), taskstatus.name AS status FROM workflows JOIN users ON workflows.user_id = users.id LEFT JOIN workitems ON workflows.id = workitems.workflow_id LEFT JOIN tasks ON workitems.id = tasks.workitem_id LEFT JOIN tasklogs ON tasks.id=tasklogs.task_id JOIN taskstatus ON tasklogs.status_id=taskstatus.id GROUP BY workflows.id HAVING users.id=' + str(current_user.id)
        sql = 'SELECT workflows.*, MAX(time), taskstatus.name AS status FROM workflows LEFT JOIN workitems ON workflows.id = workitems.workflow_id LEFT JOIN tasks ON workitems.id = tasks.workitem_id LEFT JOIN tasklogs ON tasks.id=tasklogs.task_id LEFT JOIN taskstatus ON tasklogs.status_id=taskstatus.id WHERE workflows.user_id={0} GROUP BY workflows.id'.format(current_user.id)
        workflows = db.engine.execute(sql)
    
    workitems = []
#    Workflow.query.join(WorkItem).join(Operation).filter_by(id=1).c
#    sql = text('SELECT workitems.*, operations.name AS opname FROM workflows INNER JOIN workitems ON workflows.id=workitems.workflow_id INNER join operations ON workitems.operation_id=operations.id WHERE workflows.id=' + str(id))
    
    workflow_name = ''
    if id is not None and Workflow.query.get(id) is not None:
        workflow_name = Workflow.query.get(id).name
#        sql = text('SELECT workitems.*, operations.name AS opname, datasources.id AS datasource_id, datasources.name AS datasource_name, data.url AS path FROM workflows INNER JOIN workitems ON workflows.id=workitems.workflow_id INNER join operations ON workitems.operation_id=operations.id INNER JOIN data ON workitems.id = data.id INNER JOIN datasources ON data.datasource_id=datasources.id WHERE workflows.id=' + str(id))
#        sql = text('SELECT s.name AS name, s.input AS input, s.output AS output, dx.url AS input_root, dx2.url AS output_root, dx.type AS input_type, dx2.type AS output_type, operations.name AS opname FROM (SELECT w.*, d1.datasource_id AS input_datasource, d1.url AS input, d2.datasource_id AS output_datasource, d2.url AS output FROM workitems w INNER JOIN data d1 ON d1.id=w.input_id INNER JOIN data d2 ON d2.id=w.output_id) s INNER JOIN datasources dx ON dx.id=s.input_datasource INNER JOIN datasources dx2 ON dx2.id=s.output_datasource INNER JOIN operations ON s.operation_id = operations.id INNER JOIN workflows ON s.workflow_id=workflows.id WHERE workflows.id=' + str(id))
#        sql = text('SELECT s.id AS id, s.name AS name, s.input AS input, s.output AS output, dx.url AS input_root, dx2.url AS output_root, dx.type AS input_type, dx2.type AS output_type, operations.name AS opname FROM (SELECT w.*, d1.datasource_id AS input_datasource, d1.url AS input, d2.datasource_id AS output_datasource, d2.url AS output FROM workitems w LEFT JOIN data d1 ON d1.id=w.input_id LEFT JOIN data d2 ON d2.id=w.output_id) s LEFT JOIN datasources dx ON dx.id=s.input_datasource LEFT JOIN datasources dx2 ON dx2.id=s.output_datasource LEFT JOIN operations ON s.operation_id = operations.id INNER JOIN workflows ON s.workflow_id=workflows.id WHERE workflows.id=' + str(id))
        sql = text('SELECT w.id AS id, w.name AS name, w.desc as desc, ops.name AS opsname, operations.name AS opname, d1.url AS input, d2.url AS output, dx1.id AS input_datasourceid, dx1.type AS input_datasource, dx1.url AS input_root, dx2.id AS output_datasourceid, dx2.type AS output_datasource, dx2.url AS output_root FROM workitems w LEFT JOIN operations ON w.operation_id=operations.id LEFT JOIN operationsources ops ON ops.id=operations.operationsource_id LEFT JOIN data d1 ON d1.id=w.input_id LEFT JOIN data d2 ON d2.id=w.output_id LEFT JOIN datasources dx1 ON dx1.id=d1.datasource_id LEFT JOIN datasources dx2 ON dx2.id=d2.datasource_id WHERE w.workflow_id=' + str(id))
        workitems = db.engine.execute(sql)        
#         result = db.engine.execute(sql)
#         for row in result:
#             workitems.append(row);
        
#     if id is not None:
#         workflow = Workflow.query.filter_by(id=id)
#         if workflow is not None and workflow.count() > 0:
#             workitems = workflow.first().workitems
    
    
    return render_template('index.html', form=form, posts=posts, datasources=datasource_tree, operations=operation_tree, workflow=workflow_name, workflows=workflows, workitems=workitems,
                           show_followed=show_followed, pagination=pagination)


@main.route('/user/<username>')
def user(username):
    user = User.query.filter_by(username=username).first_or_404()
    page = request.args.get('page', 1, type=int)
    pagination = user.posts.order_by(Post.timestamp.desc()).paginate(
        page, per_page=current_app.config['PHENOPROC_POSTS_PER_PAGE'],
        error_out=False)
    posts = pagination.items
    return render_template('user.html', user=user, posts=posts,
                           pagination=pagination)


@main.route('/edit-profile', methods=['GET', 'POST'])
@login_required
def edit_profile():
    form = EditProfileForm()
    if form.validate_on_submit():
        current_user.name = form.name.data
        current_user.location = form.location.data
        current_user.about_me = form.about_me.data
        db.session.add(current_user)
        flash('Your profile has been updated.')
        return redirect(url_for('.user', username=current_user.username))
    form.name.data = current_user.name
    form.location.data = current_user.location
    form.about_me.data = current_user.about_me
    return render_template('edit_profile.html', form=form)


@main.route('/edit-profile/<int:id>', methods=['GET', 'POST'])
@login_required
@admin_required
def edit_profile_admin(id):
    user = User.query.get_or_404(id)
    form = EditProfileAdminForm(user=user)
    if form.validate_on_submit():
        user.email = form.email.data
        user.username = form.username.data
        user.confirmed = form.confirmed.data
        user.role = Role.query.get(form.role.data)
        user.name = form.name.data
        user.location = form.location.data
        user.about_me = form.about_me.data
        db.session.add(user)
        flash('The profile has been updated.')
        return redirect(url_for('.user', username=user.username))
    form.email.data = user.email
    form.username.data = user.username
    form.confirmed.data = user.confirmed
    form.role.data = user.role_id
    form.name.data = user.name
    form.location.data = user.location
    form.about_me.data = user.about_me
    return render_template('edit_profile.html', form=form, user=user)


@main.route('/post/<int:id>', methods=['GET', 'POST'])
def post(id):
    post = Post.query.get_or_404(id)
    form = CommentForm()
    if form.validate_on_submit():
        comment = Comment(body=form.body.data,
                          post=post,
                          author=current_user._get_current_object())
        db.session.add(comment)
        flash('Your comment has been published.')
        return redirect(url_for('.post', id=post.id, page=-1))
    page = request.args.get('page', 1, type=int)
    if page == -1:
        page = (post.comments.count() - 1) // \
            current_app.config['PHENOPROC_COMMENTS_PER_PAGE'] + 1
    pagination = post.comments.order_by(Comment.timestamp.asc()).paginate(
        page, per_page=current_app.config['PHENOPROC_COMMENTS_PER_PAGE'],
        error_out=False)
    comments = pagination.items
    return render_template('post.html', posts=[post], form=form,
                           comments=comments, pagination=pagination)

@main.route('/workflow/<int:id>', methods=['GET', 'POST'])
def workflow(id):
    workflow = Workflow.query.get_or_404(id)
    return render_template('workflow.html', workflows=[workflow])
                           
@main.route('/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def edit(id):
    post = Post.query.get_or_404(id)
    if current_user != post.author and \
            not current_user.can(Permission.ADMINISTER):
        abort(403)
    form = PostForm()
    if form.validate_on_submit():
        post.body = form.body.data
        db.session.add(post)
        flash('The post has been updated.')
        return redirect(url_for('.post', id=post.id))
    form.body.data = post.body
    return render_template('edit_post.html', form=form)


@main.route('/follow/<username>')
@login_required
@permission_required(Permission.FOLLOW)
def follow(username):
    user = User.query.filter_by(username=username).first()
    if user is None:
        flash('Invalid user.')
        return redirect(url_for('.index'))
    if current_user.is_following(user):
        flash('You are already following this user.')
        return redirect(url_for('.user', username=username))
    current_user.follow(user)
    flash('You are now following %s.' % username)
    return redirect(url_for('.user', username=username))


@main.route('/unfollow/<username>')
@login_required
@permission_required(Permission.FOLLOW)
def unfollow(username):
    user = User.query.filter_by(username=username).first()
    if user is None:
        flash('Invalid user.')
        return redirect(url_for('.index'))
    if not current_user.is_following(user):
        flash('You are not following this user.')
        return redirect(url_for('.user', username=username))
    current_user.unfollow(user)
    flash('You are not following %s anymore.' % username)
    return redirect(url_for('.user', username=username))


@main.route('/followers/<username>')
def followers(username):
    user = User.query.filter_by(username=username).first()
    if user is None:
        flash('Invalid user.')
        return redirect(url_for('.index'))
    page = request.args.get('page', 1, type=int)
    pagination = user.followers.paginate(
        page, per_page=current_app.config['PHENOPROC_FOLLOWERS_PER_PAGE'],
        error_out=False)
    follows = [{'user': item.follower, 'timestamp': item.timestamp}
               for item in pagination.items]
    return render_template('followers.html', user=user, title="Followers of",
                           endpoint='.followers', pagination=pagination,
                           follows=follows)


@main.route('/followed-by/<username>')
def followed_by(username):
    user = User.query.filter_by(username=username).first()
    if user is None:
        flash('Invalid user.')
        return redirect(url_for('.index'))
    page = request.args.get('page', 1, type=int)
    pagination = user.followed.paginate(
        page, per_page=current_app.config['PHENOPROC_FOLLOWERS_PER_PAGE'],
        error_out=False)
    follows = [{'user': item.followed, 'timestamp': item.timestamp}
               for item in pagination.items]
    return render_template('followers.html', user=user, title="Followed by",
                           endpoint='.followed_by', pagination=pagination,
                           follows=follows)


@main.route('/all')
@login_required
def show_all():
    resp = make_response(redirect(url_for('.index')))
    resp.set_cookie('show_followed', '', max_age=30*24*60*60)
    return resp


@main.route('/followed')
@login_required
def show_followed():
    resp = make_response(redirect(url_for('.index')))
    resp.set_cookie('show_followed', '1', max_age=30*24*60*60)
    return resp


@main.route('/moderate')
@login_required
@permission_required(Permission.MODERATE_COMMENTS)
def moderate():
    page = request.args.get('page', 1, type=int)
    pagination = Comment.query.order_by(Comment.timestamp.desc()).paginate(
        page, per_page=current_app.config['PHENOPROC_COMMENTS_PER_PAGE'],
        error_out=False)
    comments = pagination.items
    return render_template('moderate.html', comments=comments,
                           pagination=pagination, page=page)


@main.route('/moderate/enable/<int:id>')
@login_required
@permission_required(Permission.MODERATE_COMMENTS)
def moderate_enable(id):
    comment = Comment.query.get_or_404(id)
    comment.disabled = False
    db.session.add(comment)
    return redirect(url_for('.moderate',
                            page=request.args.get('page', 1, type=int)))


@main.route('/moderate/disable/<int:id>')
@login_required
@permission_required(Permission.MODERATE_COMMENTS)
def moderate_disable(id):
    comment = Comment.query.get_or_404(id)
    comment.disabled = True
    db.session.add(comment)
    return redirect(url_for('.moderate',
                            page=request.args.get('page', 1, type=int)))

@main.route('/about')
def about():
    return render_template('about.html')

@main.route('/contact')
def contact():
    return render_template('contact.html')

from sqlalchemy.ext.declarative import DeclarativeMeta
class AlchemyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj.__class__, DeclarativeMeta):
            # an SQLAlchemy class
            fields = {}
            for field in [x for x in dir(obj) if not x.startswith('_') and x != 'metadata']:
                data = obj.__getattribute__(field)
                try:
                    json.dumps(data) # this will fail on non-encodable values, like other classes
                    fields[field] = data
                except TypeError:
                    fields[field] = None
            # a json-encodable dict
            return fields
    
        return json.JSONEncoder.default(self, obj)

@main.route('/tasklogs', methods=['POST'])
@login_required
def translate():
    workflow_id = request.form['text'] #request.args.get('workflow_id')
    workflow_id = Utility.ValueOrNone(workflow_id)
    if workflow_id is not None and Workflow.query.get(workflow_id) is not None:
        sql = 'SELECT workitems.id, MAX(time), taskstatus.name as status FROM workitems LEFT JOIN tasks ON workitems.id=tasks.workitem_id LEFT JOIN tasklogs ON tasklogs.task_id=tasks.id LEFT JOIN taskstatus ON tasklogs.status_id = taskstatus.id WHERE workitems.workflow_id=' + str(workflow_id) + ' GROUP BY workitems.id'
        result = db.engine.execute(sql)
        return json.dumps([dict(r) for r in result], cls=AlchemyEncoder)

@main.route('/delete', methods=['POST'])
@login_required
def delete():
    datasource_id = Utility.ValueOrNone(request.form['datasource'])
    filesystem = getFileSystem(datasource_id)
    if filesystem is not None:
        path = os.path.join(Utility.get_rootdir(datasource_id), request.form['path'])
        filesystem.delete(path)
    return json.dumps(dict())

@main.route('/rename', methods=['POST'])
@login_required
def rename():
    datasource_id = Utility.ValueOrNone(request.form['datasource'])
    filesystem = getFileSystem(datasource_id)
    if filesystem is not None:
        oldpath = os.path.join(Utility.get_rootdir(datasource_id), request.form['path'])
        
        newpath = os.path.join(os.path.dirname(oldpath), request.form['newname'])
        filesystem.rename(oldpath, newpath)
        return json.dumps(filesystem.make_json(datasource_id, Utility.get_rootdir(datasource_id), os.path.relpath(newpath, Utility.get_rootdir(datasource_id))))
    return json.dumps(dict())
       
@main.route('/addfolder', methods=['POST'])
@login_required
def addfolder():
    datasource_id = Utility.ValueOrNone(request.form['datasource'])
    filesystem = getFileSystem(datasource_id)
    if filesystem is not None:
        path = os.path.join(Utility.get_rootdir(datasource_id), request.form['path'])
        newfolder = filesystem.addfolder(path)
        return json.dumps(filesystem.make_json(datasource_id, Utility.get_rootdir(datasource_id), os.path.relpath(newfolder, Utility.get_rootdir(datasource_id))))
    return json.dumps(dict())
    
    # Route that will process the file upload
@main.route('/upload', methods=['POST'])
def upload():
    # Get the name of the uploaded file
    file = request.files['file']
    
    # Check if the file is one of the allowed types/extensions
    if file:
        datasource_id = Utility.ValueOrNone(request.form['datasource'])
        filesystem = getFileSystem(datasource_id)
        if filesystem is not None:
            # Make the filename safe, remove unsupported chars
            filename = secure_filename(file.filename)
            # Move the file form the temporal folder to
            # the upload folder we setup
            path = os.path.join(Utility.get_rootdir(datasource_id), request.form['path'], filename)         
            filesystem.saveUpload(file, path)
            
    return json.dumps({})

@main.route('/download', methods=['POST'])
@login_required
def download():
    datasource_id = Utility.ValueOrNone(request.form['datasource'])
    filesystem = getFileSystem(datasource_id)
    if filesystem is not None:
        path = os.path.join(Utility.get_rootdir(datasource_id), request.form['path'])
        if os.path.isfile(path):
            return send_from_directory(directory=os.path.dirname(path), filename=os.path.basename(path), as_attachment=True)
    return json.dumps(dict())