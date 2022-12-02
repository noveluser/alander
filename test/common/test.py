#!/usr/bin/python
# coding=utf-8


from datetime import datetime
import time
import os
from apscheduler.schedulers.background import BackgroundScheduler


def tick(a):
    # print('Tick! The time is: %s' % datetime.now())
    print(a)


def tickagain(a):
    # print('Tick! The time is: %s' % datetime.now())
    print(a)


if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(tick, 'interval', ["ok", ], seconds=2)
    scheduler.add_job(tickagain, 'interval', ["error", ], seconds=4)
    scheduler.start()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
