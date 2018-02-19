import subprocess

def func_exec_stdout(app, *args):
    cmd = app
    if args:
        cmd += ' ' + ' '.join(args)
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    return p.stdout, p.stderr

def func_exec_run(app, *args):
    out, err = func_exec_stdout(app, *args)
    return out.decode('utf-8'), err.decode('utf-8')

def func_exec(app, *args):

    cmd = app
    if args:
        cmd += ' ' + ' '.join(args)
    p = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=False)
    return p.stdout.read()