#!/usr/bin/python
# coding=utf-8


' 一个监控程序的监控程序，囧'
__author__ = 'Alex Wang'             # 标准文件模板

import os
import subprocess
import logging


env = "firstscanbags"
# multi env configurations
cfg = {
        "firstscanbags": {
            "program": "firstscanbags.py",
            "program_path": "/data/package/crontab/other/",
        },
        "secondscanbags": {
            "program": "secondscanbags.py",
            "program_path": "/data/package/crontab/other/",
        },
        "cws4job": {
            "program": "check_cws4job.py",
            "program_path": "/data/cyy928/crontab/",
        },
        "rewrite": {
            "program": "check_rewrite.py",
            "program_path": "/data/cyy928/crontab/",
        }
    }


logging.basicConfig(
                    level=logging.INFO, 
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename="/data/package/crontab/log/monitor.log",
                    # filename='c://work//log//test.log',
                    filemode='a')


def check_process_activity(node, key):
    command = "ps -A | grep {} | grep -v defunct".format(key)
    # 增加这个' | grep -v defunct '是为了筛选僵尸进程
    Pro_status = os.popen(command, 'r').readlines()
    # print(Pro_status)
    if Pro_status == []:
        logging.error("监控{}程序未能检测到，已自动重启".format(key))
        start(cfg[node]["program"], cfg[node]["program_path"])
    else:
        logging.info("监控{}程序正在运行".format(key))
        return False
    return True


def start(program, path):     # 运行shell重启脚本
    try:
        # s.system(cfg[env]['program_path']+cfg[env]['program'])
        subprocess.Popen(path+program, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         shell=True, cwd=path)
    except Exception as e:
        logging.error('启动失败,报错信息:{}'.format(e))
        return False
    # finally:
    #     result.wait()    # 不启动wait模块，那么restart_cws意外终止时，会产生一些僵尸进程
    return True


if __name__ == "__main__":
    program = ["firstscanbags", "secondscanbags"]
    for i in program:
        result = check_process_activity(i, cfg[i]["program"][0:15])
        if result:
            logging.info("已重启服务成功")
