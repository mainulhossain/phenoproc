activate_this = '/var/www/phenoproc/venv/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

import sys, os
import logging
logging.basicConfig(stream=sys.stderr)

sys.path.insert(0,"/var/www/phenoproc/")
sys.path.insert(0,"/var/www/phenoproc/static/")
os.chdir("/var/www/phenoproc")
from manage import app as application
