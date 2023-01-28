#!/usr/bin/python
# coding=utf-8


# 导入需要的库
from my_mysql import NewDatabase
from apscheduler.schedulers.background import BackgroundScheduler


# envioments
# pool = Database(pool_size=10, host='10.110.191.24', user='it', password='1111111', database='ics', port=3306)


def executemysql(query):
    with NewDatabase(pool_size=10, host='10.31.9.24', user='it', password='1111111', database='ics', port=3306) as pool:
        result = pool.run_query(query)
        # print(result[0].get("pid"))
        print(result)
    return result


def highFrequencyWord():
    query = "SELECT pid FROM  temp_bags  WHERE  (status not in ('arrived', 'dump') or status is null)  AND bsm_time >= DATE_ADD(NOW(),INTERVAL - 1 HOUR)  order by id"
    result = executemysql(query)
    return result


def collectbaginfo():
    startIDquery = "SELECT pid FROM  temp_bags  WHERE  (status not in ('arrived', 'dump') or status is null)  AND bsm_time >= DATE_ADD(NOW(),INTERVAL + 10 HOUR)  order by id"
    startID = executemysql(startIDquery)
    return startID


if __name__ == "__main__":
    recentpid_list = []
    pidResult = highFrequencyWord()
    # if pidResult:
    #     for item in pidResult:
    #         recentpid_list.append(item.values())
    # print(recentpid_list)
    # scheduler = BackgroundScheduler()
    # # 添加任务
    # # scheduler.add_job(executemysql, 'interval', [query, ], seconds=10)
    # scheduler.add_job(collectbaginfo, 'interval', seconds=5)
    # scheduler.start()
    # try:
    #     # This is here to simulate application activity (which keeps the main thread alive).
    #     while True:
    #         time.sleep(5)
    # except Exception as e:
    #     # Not strictly necessary if daemonic mode is enabled but should be done if possible
    #     print(e)
    #     scheduler.shutdown()





