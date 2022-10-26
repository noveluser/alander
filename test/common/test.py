#!/usr/bin/python
# coding=utf-8

from concurrent.futures.thread import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler
import os
  
def job1(para1, para2,para3):
    print("This is job1")
    print("The para1 is "+str(para1))
    print("The para2 is "+str(para2))
    print("The para2 is "+str(para3))
def job2(para1, para2,para3):
    print("This is job2")
    print("The para1 is "+str(para1))
    print("The para2 is "+str(para2))
    print("The para2 is "+str(para3))


exec = {
    'default': ThreadPoolExecutor(max_workers=10)
}
# 3、实例化
block_scheduer = BlockingScheduler(executor = exec)
block_scheduer.add_job(job1, 'interval', seconds=1,args=['para1','para2','para3'])
block_scheduer.add_job(job2, 'interval', seconds=2,kwargs={'para1':'3','para2':'2','para3':'1'})
print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

try:
    block_scheduer.start()
except (KeyboardInterrupt, SystemExit):
    block_scheduer.shutdown(wait=False)

