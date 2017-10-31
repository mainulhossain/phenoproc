import os
basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    DATA_DIR = os.path.join(basedir, 'storage')
    PUBLIC_DIR = 'public'
    CURRENT_USER = 'public'
    WEBHDFS_ADDR = 'http://sr-p2irc-big1.usask.ca:50070'
    WEBHDFS_USER = 'hdfs'
    HDFS_USER = 'phenodoop'
    HDFS_GROUP = 'phenodoop'
    HDFS_DIR = '/user/phenodoop'
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'hard to guess string'
    SSL_DISABLE = False
    SQLALCHEMY_COMMIT_ON_TEARDOWN = True
    SQLALCHEMY_RECORD_QUERIES = True
    #MAIL_SERVER = 'smtp.googlemail.com'
    MAIL_SERVER = 'smtp.gmail.com'
    MAIL_PORT = 587
    MAIL_USE_TLS = True
    MAIL_USE_SSL = False
    MAIL_USERNAME = 'phenoproc@gmail.com' #os.environ.get('MAIL_USERNAME') or 'phenoproc@gmail.com'
    MAIL_PASSWORD = '!phenoproc-2016!' #os.environ.get('MAIL_PASSWORD') or '!phenoproc@gmail.com!'
    broker_url = 'redis://localhost:6379/0'
    result_backend = 'redis://localhost:6379/0'
    imports = ['app.jobs']
    BIOWL = os.path.join(basedir, 'app/biowl/')
    PHENOPROC_MAIL_SUBJECT_PREFIX = '[Phenoproc]'
    PHENOPROC_MAIL_SENDER = 'Phenoproc Admin <phenoproc@gmail.com>'
    PHENOPROC_ADMIN = os.environ.get('PHENOPROC_ADMIN')         
    PHENOPROC_POSTS_PER_PAGE = 20
    PHENOPROC_FOLLOWERS_PER_PAGE = 50
    PHENOPROC_COMMENTS_PER_PAGE = 30
    PHENOPROC_SLOW_DB_QUERY_TIME=0.5
    WORKFLOW_MODE_EDIT = False 

    @staticmethod
    def init_app(app):
        pass


class DevelopmentConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = os.environ.get('DEV_DATABASE_URL') or \
        'sqlite:///' + os.path.join(basedir, 'data-dev.sqlite')


class TestingConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = os.environ.get('TEST_DATABASE_URL') or \
        'sqlite:///' + os.path.join(basedir, 'data-test.sqlite')
    WTF_CSRF_ENABLED = False


class ProductionConfig(Config):
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        'sqlite:///' + os.path.join(basedir, 'data.sqlite')

    @classmethod
    def init_app(cls, app):
        Config.init_app(app)

        # email errors to the administrators
        import logging
        from logging.handlers import SMTPHandler
        credentials = None
        secure = None
        if getattr(cls, 'MAIL_USERNAME', None) is not None:
            credentials = (cls.MAIL_USERNAME, cls.MAIL_PASSWORD)
            if getattr(cls, 'MAIL_USE_TLS', None):
                secure = ()
        mail_handler = SMTPHandler(
            mailhost=(cls.MAIL_SERVER, cls.MAIL_PORT),
            fromaddr=cls.PHENOPROC_MAIL_SENDER,
            toaddrs=[cls.PHENOPROC_ADMIN],
            subject=cls.PHENOPROC_MAIL_SUBJECT_PREFIX + ' Application Error',
            credentials=credentials,
            secure=secure)
        mail_handler.setLevel(logging.ERROR)
        app.logger.addHandler(mail_handler)


class HerokuConfig(ProductionConfig):
    SSL_DISABLE = bool(os.environ.get('SSL_DISABLE'))

    @classmethod
    def init_app(cls, app):
        ProductionConfig.init_app(app)

        # handle proxy server headers
        from werkzeug.contrib.fixers import ProxyFix
        app.wsgi_app = ProxyFix(app.wsgi_app)

        # log to stderr
        import logging
        from logging import StreamHandler
        file_handler = StreamHandler()
        file_handler.setLevel(logging.WARNING)
        app.logger.addHandler(file_handler)


class UnixConfig(ProductionConfig):
    @classmethod
    def init_app(cls, app):
        ProductionConfig.init_app(app)

        # log to syslog
        import logging
        from logging.handlers import SysLogHandler
        syslog_handler = SysLogHandler()
        syslog_handler.setLevel(logging.WARNING)
        app.logger.addHandler(syslog_handler)


config = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'heroku': HerokuConfig,
    'unix': UnixConfig,

    'default': DevelopmentConfig
}
