import subprocess
import os
from os.path import join, dirname

def run(script, *args):
    this_dir = dirname(os.path.abspath(__file__))
    cmd = join(this_dir, script)
    args = list(args)
    args.insert(0, '/usr/bin/python')
    args.insert(1, cmd)
    args = list(map(str, args))
    p = subprocess.Popen(args, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return p.communicate()

def run_hippi(*args):
    return run('img_pipe2.py', *args)

def run_shippi(*args):
    return run('shippi.py', *args)
