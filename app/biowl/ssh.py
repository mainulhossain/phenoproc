import paramiko

def ssh_command(myhost, myuser, mypassword, command):
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=myhost, username=myuser, password=mypassword)
    stdin, stdout, stderr = ssh_client.exec_command(command)
    exit_status = stdout.channel.recv_exit_status() # Channel.exit_status_ready for non-blocking call
    ssh_client.close()
    return exit_status