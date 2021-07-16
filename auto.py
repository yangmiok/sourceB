#!/usr/bin/python

import paramiko
import sys
reload(sys)

sys.setdefaultencoding( "utf-8" )

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname='192.168.38.38', port=22, username='root', password='xxx')

stdin, stdout, stderr = ssh.exec_command('/root/filecoin/cmd/lotus-miner info')

res, err = stdout.read(), stderr.read()
result = res if res else err

print(result.decode())

