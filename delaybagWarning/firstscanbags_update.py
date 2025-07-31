#!/usr/bin/python
# coding=utf-8

# 获取当日所有行李状态
# 包括空框，UFO等等
# v0.2


import cx_Oracle
import logging
import datetime
import time
from multiprocessing import Process, Queue
from my_newmysql import NewDatabase


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/firstscanbags.log',
                    # filename='c://work//log//1.log',
                    filemode='a')


def accessOracle(query):
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    c.execute(query)  # use triple quotes if you want to spread your query across multiple lines
    result = c.fetchall()
    conn.close()
    return result


def executemysql(query):
    with NewDatabase(pool_size=10, host='10.31.9.24', user='it', password='1111111', database='ics', port=3306) as pool:
        return pool.run_query(query)


async def async_collectbaginfo():
    results = await collectbaginfo()
    return results


def collectbaginfo():
    startIDquery = "select idnumber from commonidrecord where checktablename = 'WC_PACKAGEINFO' and user = 'firstscanbags' "
    try:
        startID = executemysql(startIDquery)[0].get("idnumber")
    except Exception as e:
        logging.error(e)
    endIDquery = "select max(IDEVENT) from WC_PACKAGEINFO"
    endID = accessOracle(endIDquery)[0][0]
    pendingID = endID - startID
    if pendingID > 5000:
        startID = endID - 5000
        logging.info("有{}条packaginfo记录被丢弃".format(pendingID - 5000))
    logging.info("搜索到{}条packaginfo记录".format(pendingID))
    updateIDnumber = "update ics.commonidrecord set IDnumber= {} where checktablename = 'WC_PACKAGEINFO' and user = 'firstscanbags' ".format(endID)
    executemysql(updateIDnumber)
    '''先尽快完成实体，先跳过SQL写法,先用多次SQL查询，效率上会有影响，数据查询不会有问题'''
    sqlquery = "SELECT 	 EVENTTS,lpc, pid, CURRENTSTATIONID,L_DESTINATIONSTATIONID, flightnr,IDEVENT  FROM 	( SELECT EVENTTS, IDEVENT, lpc, pid, CURRENTSTATIONID,L_DESTINATIONSTATIONID,  	( DEPAIRLINE || DEPFLIGHT ) flightnr, ROW_NUMBER ( ) OVER ( PARTITION BY pid ORDER BY IDEVENT ) AS rn  FROM 	WC_PACKAGEINFO  WHERE 	IDEVENT > {}  AND IDEVENT <= {} 	AND EXECUTEDTASK = 'AutoScan'  	)  WHERE 	rn = 1  ORDER BY pid ".format(startID, endID)
    logging.info(sqlquery)
    # 注意，我这里筛选了EXECUTEDTASK = 'AutoScan'，暂不知是否存在没有autoscan，但是行李正常的情况，也许存在中转行李这种情况，后面再查
    data = accessOracle(sqlquery)
    logging.info("本次操作{}条记录需要被输入".format(len(data)))
    for row in data:
        # 如果pid不在current_bags表里，那么加入，这个表需要定时清除
        logging.info(row)
        search_pid_query = "select pid from current_bags where pid = {} and TO_DAYS(create_time) >  TO_DAYS(NOW())-1 ;".format(row[2])
        if not executemysql(search_pid_query):
            create_time = row[0].strftime("%Y-%m-%d %H:%M:%S.%f")
            # 日后研究有无直接用oracle下的timestamp格式的字段
            add_pid_query_current_bags = "insert into ics.current_bags (create_time, pid)  values ('{}',{})".format(create_time, row[2])
            executemysql(add_pid_query_current_bags)
            logging.info("mysql语句{}已执行".format(add_pid_query_current_bags))
            # 再加入temp_bags里
            lpc = row[1] or 'NULL'
            flightnr = row[5] or 'NULL'
            add_pid_query_temp_bags = "INSERT INTO ics.temp_bags (bsm_time, lpc, pid, current_location, orginal_destination, flightnr) VALUES ('{}', {}, {}, '{}', '{}', '{}')".format(create_time, lpc, row[2], row[3], row[4], flightnr)
            optimizal_add_pid_query_temp_bags = add_pid_query_temp_bags.replace("None", "Null")
            # logging.info(add_pid_query_temp_bags)
            executemysql(optimizal_add_pid_query_temp_bags)
            logging.info("write {} to temp_bags".format(row[2]))
        else:
            logging.info("pass {}".format(row[2]))
        #     searchlpcquery = "WITH ar AS ( SELECT * FROM WC_PACKAGEINFO WHERE pid = {} AND EXECUTEDTASK IS NOT NULL ) , br as ( SELECT EVENTTS, lpc, pid, ( DEPAIRLINE || DEPFLIGHT ) flightnr, CURRENTSTATIONID, L_DESTINATIONSTATIONID  FROM WC_PACKAGEINFO  WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM ar ) ) select br.*,std FROM br left join FACT_FLIGHT_SUMMARIES_V ffs on br.FLIGHTNR = ffs.FLIGHTNR  AND ffs.STD > TRUNC( SYSDATE +8/24 )".format(row[0])
        #     # baginfo格式EVENTTS, lpc, pid, flightnr, CURRENTSTATIONID, L_DESTINATIONSTATIONID，STD
        #     baginfo = accessOracle(searchlpcquery)
        #     for item in baginfo:
        #         if item[1] and item[1] != 0:       # 存在LPC可能拥有多个PID的情况，此时以LPC为准。但MCS会输入特殊LPC000000，所以必须不为0
        #             searchbag = "select lpc from ics.temp_bags where lpc = {} and TO_DAYS(bsm_time) > TO_DAYS(NOW())-2 ".format(item[1])
        #         else:
        #             searchbag = "select pid from ics.temp_bags where pid = {} and TO_DAYS(bsm_time) > TO_DAYS(NOW())-2 ".format(item[2])
        #         identification = executemysql(searchbag)
        #         if not identification:
        #             if not item[6]:     # 为解决字符串为空时无法写入sql的问题的临时解决办法
        #                 STD = 'Null'
        #             else:
        #                 STD = "'{}'".format(item[6])
        #             if not item[3]:
        #                 flightnr = 'Null'
        #             else:
        #                 flightnr = "'{}'".format(item[3])
        #             localBMSTime = item[0] + datetime.timedelta(hours=8)
        #             register_time = localBMSTime.strftime("%Y-%m-%d %H:%M:%S.%f")
        #             orignal_sqlquery = "insert into ics.temp_bags (bsm_time, lpc, pid, flightnr, current_location, orginal_destination, STD)  values ('{}',{},{},{},'{}','{}',{})".format(register_time, item[1], item[2], flightnr, item[4], item[5], STD)
        #             optimizal_sqlquery = orignal_sqlquery.replace("None", "Null")
        #             logging.info(optimizal_sqlquery)
        #             executemysql(optimizal_sqlquery)
        #             if not item[4]:
        #                 logging.error("异常 {}".format(item))
        # else:
        #     logging.info("pass {}".format(row[0]))

if __name__ == '__main__':
    check_interval = 5  # 可配置参数
    while True:
        try:
            start_time = time.time()
            collectbaginfo()
            elapsed = time.time() - start_time
            # 动态调整休眠时间保证间隔准确
            sleep_time = max(0, check_interval - elapsed)
            logging.info(f"检查完成，耗时{elapsed:.2f}秒，下次检查将在{sleep_time}秒后")
            time.sleep(sleep_time)
        except Exception as e:
            logging.error(f"检查过程中发生异常: {str(e)}", exc_info=True)
            # 异常后保持运行，可根据需要调整重试策略
            time.sleep(10)  # 短暂休眠后重试


# def worker(queue):
#     while True:
#         try:
#             collectbaginfo()
#             queue.put({"status": "success", "timestamp": time.time()})
#         except Exception as e:
#             queue.put({"status": "error", "message": str(e)})


# if __name__ == '__main__':
#     queue = Queue()
#     p = Process(target=worker, args=(queue,))
#     p.start()
    
#     try:
#         while True:
#             # 设置超时（如1秒），避免主进程无意义等待
#             result = queue.get(timeout=600)
#             if result["status"] == "success":
#                 print(f"检查成功，耗时{result.get('cost', 0):.2f}秒")
#             else:
#                 print(f"检查失败: {result['message']}\n{result['traceback']}")
#             time.sleep(1)
#     except KeyboardInterrupt:
#         p.terminate()
#         p.join()  # 等待子进程退出