#!/usr/bin/python
# coding=utf-8
'''
Created on 2019年1月29日

@author: wangle
'''
import socket
import subprocess
import shlex
import logging


# 参数配置
command_path = 'c://work//log//'
fileList = ["1.py", "2.py"]


logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='c://work//log//test.log',
        filemode='a')

if __name__ == '__main__':
    for fileName in fileList:
        shell_cmd = command_path+fileName
        cmd = shlex.split(shell_cmd)
        result = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while result.poll() is None:
            line = result.stdout.readline()
            line = line.strip()
            if line:
                logging.info('Subprogram output: [{}]'.format(line))
        if result.returncode == 0:
            logging.info('Subprogram %s success' % fileName)
        else:
            logging.info('Subprogram %s failed' % fileName)