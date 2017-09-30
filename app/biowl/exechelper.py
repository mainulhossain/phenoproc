import subprocess

def func_exec_stdout(app, *args):
    cmd = app
    if args:
        cmd += ' ' + ' '.join(args)
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    return p.stdout

def func_exec_run(app, *args):
    return func_exec_stdout(app, *args).decode('utf-8')

def func_exec(app, *args):

    cmd = app
    if args:
        cmd += ' ' + ' '.join(args)
    p = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=False)
    return p.stdout.read()