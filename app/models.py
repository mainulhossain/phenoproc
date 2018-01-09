from datetime import datetime
import hashlib
from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from markdown import markdown
import bleach
from flask import current_app, request, url_for
from flask_login import UserMixin, AnonymousUserMixin
from app.exceptions import ValidationError
from . import db, login_manager
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.engine import default
from flask_login import current_user
import os
from sqlalchemy.orm import backref

class Permission:
    FOLLOW = 0x01
    COMMENT = 0x02
    WRITE_ARTICLES = 0x04
    WRITE_WORKFLOWS = 0x08
    MODERATE_COMMENTS = 0x10
    MODERATE_WORKFLOWS = 0x20
    ADMINISTER = 0x80


class Role(db.Model):
    __tablename__ = 'roles'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), unique=True)
    default = db.Column(db.Boolean, default=False, index=True)
    permissions = db.Column(db.Integer)
    users = db.relationship('User', backref='role', lazy='dynamic')

    @staticmethod
    def insert_roles():
        roles = {
            'User': (Permission.FOLLOW |
                     Permission.COMMENT |
                     Permission.WRITE_ARTICLES |
                     Permission.WRITE_WORKFLOWS, True),
            'Moderator': (Permission.FOLLOW |
                          Permission.COMMENT |
                          Permission.WRITE_ARTICLES |
                          Permission.WRITE_WORKFLOWS |
                          Permission.MODERATE_COMMENTS |
                          Permission.MODERATE_WORKFLOWS, False),
            'Administrator': (0xff, False)
        }
        for r in roles:
            role = Role.query.filter_by(name=r).first()
            if role is None:
                role = Role(name=r)
            role.permissions = roles[r][0]
            role.default = roles[r][1]
            db.session.add(role)
        db.session.commit()

    def __repr__(self):
        return '<Role %r>' % self.name


class Follow(db.Model):
    __tablename__ = 'follows'
    follower_id = db.Column(db.Integer, db.ForeignKey('users.id'),
                            primary_key=True)
    followed_id = db.Column(db.Integer, db.ForeignKey('users.id'),
                            primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)


class User(UserMixin, db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(64), unique=True, index=True)
    username = db.Column(db.String(64), unique=True, index=True)
    role_id = db.Column(db.Integer, db.ForeignKey('roles.id'))
    password_hash = db.Column(db.String(128))
    confirmed = db.Column(db.Boolean, default=False)
    name = db.Column(db.String(64))
    location = db.Column(db.String(64))
    about_me = db.Column(db.Text())
    member_since = db.Column(db.DateTime(), default=datetime.utcnow)
    last_seen = db.Column(db.DateTime(), default=datetime.utcnow)
    avatar_hash = db.Column(db.String(32))
    posts = db.relationship('Post', backref='author', lazy='dynamic')
    workflows = db.relationship('Workflow', backref='user', lazy='dynamic')
    datasource_allocation = db.relationship('DataSourceAllocation', backref='user', lazy='dynamic')
    followed = db.relationship('Follow',
                               foreign_keys=[Follow.follower_id],
                               backref=db.backref('follower', lazy='joined'),
                               lazy='dynamic',
                               cascade='all, delete-orphan')
    followers = db.relationship('Follow',
                                foreign_keys=[Follow.followed_id],
                                backref=db.backref('followed', lazy='joined'),
                                lazy='dynamic',
                                cascade='all, delete-orphan')
    comments = db.relationship('Comment', backref='author', lazy='dynamic')

    @staticmethod
    def generate_fake(count=100):
        from sqlalchemy.exc import IntegrityError
        from random import seed
        import forgery_py

        seed()
        for i in range(count):
            u = User(email=forgery_py.internet.email_address(),
                     username=forgery_py.internet.user_name(True),
                     password=forgery_py.lorem_ipsum.word(),
                     confirmed=True,
                     name=forgery_py.name.full_name(),
                     location=forgery_py.address.city(),
                     about_me=forgery_py.lorem_ipsum.sentence(),
                     member_since=forgery_py.date.date(True))
            db.session.add(u)
            try:
                db.session.commit()
            except IntegrityError:
                db.session.rollback()

    @staticmethod
    def add_self_follows():
        for user in User.query.all():
            if not user.is_following(user):
                user.follow(user)
                db.session.add(user)
                db.session.commit()

    def __init__(self, **kwargs):
        super(User, self).__init__(**kwargs)
        if self.role is None:
            if self.email == current_app.config['PHENOPROC_ADMIN']:
                self.role = Role.query.filter_by(permissions=0xff).first()
            if self.role is None:
                self.role = Role.query.filter_by(default=True).first()
        if self.email is not None and self.avatar_hash is None:
            self.avatar_hash = hashlib.md5(
                self.email.encode('utf-8')).hexdigest()
        self.followed.append(Follow(followed=self))

    @property
    def password(self):
        raise AttributeError('password is not a readable attribute')

    @password.setter
    def password(self, password):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        return check_password_hash(self.password_hash, password)

    def generate_confirmation_token(self, expiration=3600):
        s = Serializer(current_app.config['SECRET_KEY'], expiration)
        return s.dumps({'confirm': self.id})

    def confirm(self, token):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token)
        except:
            return False
        if data.get('confirm') != self.id:
            return False
        self.confirmed = True
        db.session.add(self)
        return True

    def generate_reset_token(self, expiration=3600):
        s = Serializer(current_app.config['SECRET_KEY'], expiration)
        return s.dumps({'reset': self.id})

    def reset_password(self, token, new_password):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token)
        except:
            return False
        if data.get('reset') != self.id:
            return False
        self.password = new_password
        db.session.add(self)
        return True

    def generate_email_change_token(self, new_email, expiration=3600):
        s = Serializer(current_app.config['SECRET_KEY'], expiration)
        return s.dumps({'change_email': self.id, 'new_email': new_email})

    def change_email(self, token):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token)
        except:
            return False
        if data.get('change_email') != self.id:
            return False
        new_email = data.get('new_email')
        if new_email is None:
            return False
        if self.query.filter_by(email=new_email).first() is not None:
            return False
        self.email = new_email
        self.avatar_hash = hashlib.md5(
            self.email.encode('utf-8')).hexdigest()
        db.session.add(self)
        return True

    def can(self, permissions):
        return self.role is not None and \
            self.role.permissions is not None and \
            (self.role.permissions & permissions) == permissions

    def is_administrator(self):
        return self.can(Permission.ADMINISTER)

    def ping(self):
        self.last_seen = datetime.utcnow()
        db.session.add(self)

    def gravatar(self, size=100, default='identicon', rating='g'):
        if request.is_secure:
            url = 'https://secure.gravatar.com/avatar'
        else:
            url = 'http://www.gravatar.com/avatar'
        hash = self.avatar_hash or hashlib.md5(
            self.email.encode('utf-8')).hexdigest()
        return '{url}/{hash}?s={size}&d={default}&r={rating}'.format(
            url=url, hash=hash, size=size, default=default, rating=rating)

    def follow(self, user):
        if not self.is_following(user):
            f = Follow(follower=self, followed=user)
            db.session.add(f)

    def unfollow(self, user):
        f = self.followed.filter_by(followed_id=user.id).first()
        if f:
            db.session.delete(f)

    def is_following(self, user):
        return self.followed.filter_by(
            followed_id=user.id).first() is not None

    def is_followed_by(self, user):
        return self.followers.filter_by(
            follower_id=user.id).first() is not None

    @property
    def followed_posts(self):
        return Post.query.join(Follow, Follow.followed_id == Post.author_id)\
            .filter(Follow.follower_id == self.id)

    def to_json(self):
        json_user = {
            'url': url_for('api.get_user', id=self.id, _external=True),
            'username': self.username,
            'member_since': self.member_since,
            'last_seen': self.last_seen,
            'posts': url_for('api.get_user_posts', id=self.id, _external=True),
            'followed_posts': url_for('api.get_user_followed_posts',
                                      id=self.id, _external=True),
            'post_count': self.posts.count()
        }
        return json_user

    def generate_auth_token(self, expiration):
        s = Serializer(current_app.config['SECRET_KEY'],
                       expires_in=expiration)
        return s.dumps({'id': self.id}).decode('ascii')

    @staticmethod
    def verify_auth_token(token):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token)
        except:
            return None
        return User.query.get(data['id'])

    def __repr__(self):
        return '<User %r>' % self.username


class AnonymousUser(AnonymousUserMixin):
    def can(self, permissions):
        return False

    def is_administrator(self):
        return False

login_manager.anonymous_user = AnonymousUser


@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


class Post(db.Model):
    __tablename__ = 'posts'
    id = db.Column(db.Integer, primary_key=True)
    body = db.Column(db.Text)
    body_html = db.Column(db.Text)
    timestamp = db.Column(db.DateTime, index=True, default=datetime.utcnow)
    author_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    comments = db.relationship('Comment', backref='post', lazy='dynamic')

    @staticmethod
    def generate_fake(count=100):
        from random import seed, randint
        import forgery_py

        seed()
        user_count = User.query.count()
        for i in range(count):
            u = User.query.offset(randint(0, user_count - 1)).first()
            p = Post(body=forgery_py.lorem_ipsum.sentences(randint(1, 5)),
                     timestamp=forgery_py.date.date(True),
                     author=u)
            db.session.add(p)
            db.session.commit()

    @staticmethod
    def on_changed_body(target, value, oldvalue, initiator):
        allowed_tags = ['a', 'abbr', 'acronym', 'b', 'blockquote', 'code',
                        'em', 'i', 'li', 'ol', 'pre', 'strong', 'ul',
                        'h1', 'h2', 'h3', 'p']
        target.body_html = bleach.linkify(bleach.clean(
            markdown(value, output_format='html'),
            tags=allowed_tags, strip=True))

    def to_json(self):
        json_post = {
            'url': url_for('api.get_post', id=self.id, _external=True),
            'body': self.body,
            'body_html': self.body_html,
            'timestamp': self.timestamp,
            'author': url_for('api.get_user', id=self.author_id,
                              _external=True),
            'comments': url_for('api.get_post_comments', id=self.id,
                                _external=True),
            'comment_count': self.comments.count()
        }
        return json_post

    @staticmethod
    def from_json(json_post):
        body = json_post.get('body')
        if body is None or body == '':
            raise ValidationError('post does not have a body')
        return Post(body=body)


db.event.listen(Post.body, 'set', Post.on_changed_body)


class Comment(db.Model):
    __tablename__ = 'comments'
    id = db.Column(db.Integer, primary_key=True)
    body = db.Column(db.Text)
    body_html = db.Column(db.Text)
    timestamp = db.Column(db.DateTime, index=True, default=datetime.utcnow)
    disabled = db.Column(db.Boolean)
    author_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    post_id = db.Column(db.Integer, db.ForeignKey('posts.id'))

    @staticmethod
    def on_changed_body(target, value, oldvalue, initiator):
        allowed_tags = ['a', 'abbr', 'acronym', 'b', 'code', 'em', 'i',
                        'strong']
        target.body_html = bleach.linkify(bleach.clean(
            markdown(value, output_format='html'),
            tags=allowed_tags, strip=True))

    def to_json(self):
        json_comment = {
            'url': url_for('api.get_comment', id=self.id, _external=True),
            'post': url_for('api.get_post', id=self.post_id, _external=True),
            'body': self.body,
            'body_html': self.body_html,
            'timestamp': self.timestamp,
            'author': url_for('api.get_user', id=self.author_id,
                              _external=True),
        }
        return json_comment

    @staticmethod
    def from_json(json_comment):
        body = json_comment.get('body')
        if body is None or body == '':
            raise ValidationError('comment does not have a body')
        return Comment(body=body)


db.event.listen(Comment.body, 'set', Comment.on_changed_body)
  
#Phenodoop,Folder, Custom
class DataSource(db.Model):
    __tablename__ = 'datasources'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100))
    type = db.Column(db.String(30), nullable=True)
    url = db.Column(db.Text, nullable=True)
    
    @staticmethod
    def insert_datasources():
        datasrc = DataSource(name='Phenodoop', type='Hadoop', url='hdfs://sr-p2irc-big1.usask.ca:8020')
        db.session.add(datasrc)
        datasrc = DataSource(name='Folder', type='FileSystem', url='/var/www/phenoproc/userdata')
        db.session.add(datasrc)
        datasrc = DataSource(name='One Drive', type='Cloud', url='sr-p2irc-big6.usask.ca:8020')
        db.session.add(datasrc)
        db.session.commit()

    def __repr__(self):
        return '<DataSource %r>' % self.name

class OperationSource(db.Model):
    __tablename__ = 'operationsources'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100))
    desc = db.Column(db.Text, nullable=True)
    operations = db.relationship('Operation', backref='operationsource', lazy='dynamic')
    
    @staticmethod
    def insert_operationsources():
        opsrc = OperationSource(name='Phenodoop', desc='Various operations run primarily on Hadoop scheduler.')
        db.session.add(opsrc)
        opsrc = OperationSource(name='QIIME', desc='Various QIIME operations.')
        db.session.add(opsrc)
        opsrc = OperationSource(name='Mothur', desc='Various Mothur.')
        db.session.add(opsrc)
        opsrc = OperationSource(name='Custom', desc='Various custom operations.')
        db.session.add(opsrc)
        db.session.commit()

    def __repr__(self):
        return '<OperationSource %r>' % self.name

class Operation(db.Model):
    __tablename__ = 'operations'
    id = db.Column(db.Integer, primary_key=True)
    parent_id = db.Column(db.Integer, db.ForeignKey('operations.id'))
    operationsource_id = db.Column(db.Integer, db.ForeignKey('operationsources.id'))
    name = db.Column(db.String(100))
    desc = db.Column(db.Text)
    example = db.Column(db.Text)
    children = db.relationship("Operation", cascade="all, delete-orphan", backref=db.backref("parent", remote_side=id), collection_class=attribute_mapped_collection('name'))
#    workitems = db.relationship('WorkItem', backref='operation', lazy='dynamic')
    
    def to_json(self):
        json_post = {
            'operationsource' : self.operationsource_id,
            'name': self.name,
            'desc': self.desc,
            'example': self.desc,
        }
        return json_post

    @staticmethod
    def from_json(json_post):
        name = json_post.get('name')
        if name is None or name == '':
            raise ValidationError('Operation does not have a name')
        return Operation(name=name)

class Workflow(db.Model):
    __tablename__ = "workflows"
    id = db.Column(db.Integer, primary_key=True)
    parent_id = db.Column(db.Integer, db.ForeignKey('workflows.id'), default=-1)
#    parent = db.relationships('WorkItem', remote_side=[id])
    timestamp = db.Column(db.DateTime, index=True, default=datetime.utcnow)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    name = db.Column(db.String(100))
    desc = db.Column(db.Text, nullable=True)
    public = db.Column(db.Boolean, default=False)
    workitems = db.relationship('WorkItem', cascade="all,delete-orphan", backref='workflows', lazy='dynamic')
    children = db.relationship("Workflow", cascade="all, delete-orphan", backref=db.backref("parent", remote_side=id), collection_class=attribute_mapped_collection('name'))
   
    def to_json(self):
        json_post = {
            'name': self.name,
            'desc': self.desc,
            'timestamp': self.timestamp,
            'user': url_for('api.get_user', id=self.user_id, _external=True)
        }
        return json_post

    @staticmethod
    def from_json(json_post):
        name = json_post.get('name')
        if name is None or body == '':
            raise ValidationError('workflow does not have a name')
        return Workflow(name=name)
    
#db.event.listen(Workflow.body, 'set', Workflow.on_changed_body)

# class WorkItemDataLink(db.Model):
#     __tablename__ = 'workitem_data_link'
#     workitem_id = db.Column(db.Integer, db.ForeignKey('workitems.id'), primary_key=True)
#     data_id = db.Column(db.Integer, db.ForeignKey('data.id'), primary_key=True)
#     extra_data = db.Column(db.String(20))
#     workitem = db.relationship('WorkItem', backref=db.backref("workitems_assoc"))
#     data = db.relationship('Data', backref=db.backref("data_assoc"))
   
class WorkItem(db.Model):
    __tablename__ = 'workitems'
    id = db.Column(db.Integer, primary_key=True)
    workflow_id = db.Column(db.Integer, db.ForeignKey('workflows.id'))
    name = db.Column(db.String(100))
    desc = db.Column(db.Text, nullable=True)
    input_id = db.Column(db.Integer, ForeignKey('data.id'), nullable=True)
    output_id = db.Column(db.Integer, ForeignKey('data.id'), nullable=True)
    operation_id = db.Column(db.Integer, ForeignKey('operations.id'), nullable=True)
    inputs = db.relationship('Data', foreign_keys=[input_id])
    outputs = db.relationship('Data', foreign_keys=[output_id])
    operation = db.relationship('Operation', foreign_keys=[operation_id])
    
#     inputs = db.relationship('Data', secondary='workitem_data_link')
#     outputs = db.relationship('Data', secondary='workitem_data_link', primaryjoin= workitem_data_link.extra_data == 'output')
    
    def to_json(self):
        json_post = {
            'url': url_for('api.get_post', id=self.id, _external=True),
            'name': self.name,
            'desc': self.desc,
            'timestamp': self.timestamp,
        }
        return json_post

    @staticmethod
    def from_json(json_post):
        name = json_post.get('name')
        if name is None or name == '':
            raise ValidationError('WorkItem does not have a name')
        return WorkItem(name=name)
    
#class OperationDataLink(db.Model):
#    __table_name__ = "workitem_data_link"
#    workitem_id = db.Column(db.Integer, db.ForeignKey('workitems.id'), primary_key=True)
#    data_id = db.Column(db.Integer, db.ForeignKey('data.id'), primary_key=True)

class DataType:
    Unknown = 0x00
    Folder = 0x01
    File = 0x02
    Image = 0x04
    Video = 0x08
    Binary = 0x10
    Text = 0x20
    CSV = 0x40
    SQL = 0x80
    Custom = 0x100
    Root = 0x200
    
class DataSourceAllocation(db.Model):
     __tablename__ = 'datasource_allocations'  
     id = db.Column(db.Integer, primary_key=True)
     user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
     datasource_id = db.Column(db.Integer, db.ForeignKey('datasources.id'))
     url = db.Column(db.Text) # part added to the data source url

class Data(db.Model):
    __tablename__ = 'data'
    id = db.Column(db.Integer, primary_key=True)
    datasource_id = db.Column(db.Integer, db.ForeignKey('datasources.id'), nullable=True)
    datatype = db.Column(db.Integer)
    url = db.Column(db.Text)

class TaskStatus(db.Model):
    __tablename__ = 'taskstatus'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(30))
    
    def to_json(self):
        return {
            'id': self.id,
            'name': self.name
            }
#     Unknown = 0x00
#     Created = 0x01
#     Running = 0x02
#     Completed = 0x03
#     Faulted = 0x04
#     Cancelled = 0x05


class Task(db.Model):
    __tablename__ = 'tasks'
    id = db.Column(db.Integer, primary_key=True)
    workitem_id = db.Column(db.Integer, db.ForeignKey('workitems.id'))
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    tasklogs = db.relationship('TaskLog', cascade="all,delete-orphan", backref='tasks', lazy='dynamic')

    @staticmethod
    def create_task(workitem_id):
        task = Task()
        task.workitem_id = workitem_id
        task.tasklogs.append(TaskLog(status=TaskStatus.query.get(2))) # 2 = Created
        db.session.add(task)
        db.session.commit()
        return task.id
    
    def add_log(self, status):
        self.tasklogs.append(TaskLog(status=status))
        db.session.commit()
        
class TaskLog(db.Model):
    __tablename__ = 'tasklogs'
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.Integer, db.ForeignKey('tasks.id'))
    status_id = db.Column(db.Integer, db.ForeignKey('taskstatus.id'))
    status = db.relationship('TaskStatus', backref='tasklogs')
    time = db.Column(db.DateTime, default=datetime.utcnow)
    def updateTime(self):
        self.time = datetime.utcnow()
        db.session.add(self)

class Status:
    PENDING = 'PENDING'
    RECEIVED = 'RECEIVED'
    STARTED = 'STARTED'
    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'
    REVOKED = 'REVOKED'
    RETRY = 'RETRY'
    
class Runnable(db.Model):
    __tablename__ = 'runnables'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    celery_id = db.Column(db.String(64))
    name = db.Column(db.String(64))
    status = db.Column(db.String(30), default=Status.PENDING)
    script = db.Column(db.Text)
    out = db.Column(db.Text)
    err = db.Column(db.Text)
    duration = db.Column(db.Integer, default = 0)
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    modified_on = db.Column(db.DateTime, default=datetime.utcnow)
    
    def updateTime(self):
        self.modified_on = datetime.utcnow()
        db.session.commit()
    
    def update_status(self, status):
        self.status = status
        self.modified_on = datetime.utcnow()
        db.session.commit()
    
    def update(self):
        self.modified_on = datetime.utcnow()
        db.session.commit()
    
    def to_json(self):
        return {
            'id': self.id,
            'name': self.name,
            'script': self.script,
            'status': self.status,
            'out': self.out,
            'err': self.err,
            'duration': self.duration
        }    
    
    def to_json_info(self):
        return {
            'id': self.id,
            'name': self.name,
            'script': self.script,
            'status': self.status
        }
        
    @staticmethod
    def create_runnable(user_id):
        runnable = Runnable()
        runnable.user_id = user_id
        runnable.status = Status.PENDING
        db.session.add(runnable)
        db.session.commit()
        return runnable.id