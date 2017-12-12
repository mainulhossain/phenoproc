import paramiko

def ssh_command(myhost, myuser, mypassword, command):
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=myhost, username=myuser, password=mypassword)
    return ssh_client.exec_command(command)
