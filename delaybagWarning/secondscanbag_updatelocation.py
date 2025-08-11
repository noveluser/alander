#!/usr/bin/python
# coding=utf-8

# 更新当日所有新增行李位置
#
# v0.2


import cx_Oracle
import traceback
import logging
import datetime
import time
from my_mysql import Database
from multiprocessing import Process, Queue
# from apscheduler.schedulers.background import BackgroundScheduler


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/secondscanbags_location.log',
                    filemode='a')


# envioments
cursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')


def accessOracle(query):
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    c.execute(query)  # use triple quotes if you want to spread your query across multiple lines
    result = c.fetchall()
    conn.close()
    return result


def updatebaglocation(process_id):
    offset_value = process_id * 100
    # findbagquery = "select lpc, pid, latest_time, status, flightnr from temp_bags where status {} and bsm_time >= DATE_ADD(NOW(),INTERVAL - 3 HOUR) and flighttype = 'D' and lpc is not null".format(scanqueuenumber)
    findbagquery = f"""
            select lpc, pid, latest_time, status, flightnr, checked
            FROM (
                select 
                    lpc, 
                    pid, 
                    latest_time, 
                    status, 
                    flightnr, 
                    checked,
                    ROW_NUMBER() OVER (PARTITION BY lpc ORDER BY latest_time DESC) AS rn
                FROM temp_bags
                WHERE 
                    status not in ('arrived', 'dump')
                    AND bsm_time >= DATE_ADD(NOW(), INTERVAL - 3 HOUR )
                    AND flighttype = 'D'
                    AND lpc IS NOT NULL
            ) AS subquery
            WHERE rn = 1
            order by pid
            LIMIT 100 offset {offset_value};"""
    data = cursor.run_query(findbagquery)
    logging.info("本次更新计划存在{}个行李".format(len(data)))
    for row in data:
        sub_start_time = time.time()
        if row[0]:
            ID = "lpc = {}".format(row[0])
            judging_condition = "lpc"
            searchlpcquery = "WITH baginfo AS ( SELECT CURRENTSTATIONID, IDEVENT, pid, lpc FROM WC_PACKAGEINFO WHERE {} ), maxbaginfo AS ( SELECT * FROM baginfo WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM baginfo ) ), trackinginfo AS (  SELECT   IDEVENT,   lpc,   pid,   EVENTTS,   AREAID,   ZONEID,   EQUIPMENTID,   L_DESTINATION,   L_CARRIER,   PROCESSPLANIDNAME   FROM   WC_TRACKINGREPORT track   WHERE   {}   ),  maxtrakinginfo AS ( SELECT * FROM trackinginfo WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM trackinginfo ) ) SELECT  CURRENTSTATIONID,  maxbaginfo.lpc,  maxbaginfo.pid,  EVENTTS,  ( AREAID || '.' || ZONEID || '.' || EQUIPMENTID ) location,  L_DESTINATION,  L_CARRIER,  PROCESSPLANIDNAME  FROM  maxbaginfo  LEFT JOIN maxtrakinginfo ON maxbaginfo.{} = maxtrakinginfo.{}".format(ID, ID, judging_condition, judging_condition)
            baginfo = accessOracle(searchlpcquery)
            for item in baginfo:
                logging.info("{} location update".format(item))
                if item[5] is None:
                    logging.error("destination为空。{}".format(item))
                # item格式为CURRENTSTATIONID, lpc, pid, EVENTTS, location, L_DESTINATION, L_CARRIER,flightnr
                # 当packageinfo未找到lpc及flightnr时，要从trackingreport中补充
                if int(item[0]) in [41, 42, 81, 82, 220, 221]:    # 已到达弃包处
                    status = "dump"
                elif item[0] == item[5]:
                    status = "arrived"
                elif int(item[0]) in [100, 110, 200, 210]:
                    status = "store"
                else:
                    status = "unkonwn"
                if item[4] in ["12.43.1", "13.43.1", "28.43.1", "29.43.1"]:   # 当最新位置是43.1时，就是去MCS
                    status = "mcs"
                if item[3] or status == 'arrived':     # 如果有更新记录或行李状态发生变化时，则update
                    logging.info("pid={},locate={}".format(item[2], item[3]))
                    localtime = item[3] + datetime.timedelta(hours=8)
                    latest_time = localtime.strftime("%Y-%m-%d %H:%M:%S.%f")
                    # 当行李安检未通过时，有LPC，但行李不会进入系统，可能去开包间，这时WC_TRACKINGREPORT不会有记录，存在有LPC，但无track记录的情况，这时如果status已更新，不需要更新记录
                    if row[2] != localtime:       # 存在更新的记录,注意row[2]是timestamp，而latest_time是字符串，不能直接比较

                        if item[6]:    # 如果有托盘号，取数字，否则直接取None
                            tubid = int(item[6].split(",")[0][3:])
                        else:
                            tubid = 'Null'
                        updatebagstatus = "update temp_bags set latest_time = '{}', current_location='{}',final_destination = '{}', tubid = {}, status = '{}' where {}".format(latest_time,  item[4], item[5], tubid, status, ID)
                        optimizal_sqlquery = updatebagstatus.replace("None", "Null")
                        logging.info(optimizal_sqlquery)
                        '''每个execute前加上互斥锁'''
                        # lock = threading.Lock()
                        # lock.acquire()
                        # result = cursor.run_query(optimizal_sqlquery)
                        # lock.release()
                        result = cursor.run_query(optimizal_sqlquery)
                        logging.info("{} 位置已更新至{},uptade result:{}".format(ID, item[4], result))
                    elif status != row[3]:  # 如果不存在更新记录，但行李状态发生变化，只更新状态.这种情况发生在packageinfo更新记录速度比tracking慢的时候
                        updatebagstatus = "update temp_bags set status = '{}' where {}".format(status, ID)
                        optimizal_sqlquery = updatebagstatus.replace("None", "Null")
                        logging.info(optimizal_sqlquery)
                        '''每个execute前加上互斥锁'''
                        # lock = threading.Lock()
                        # lock.acquire()
                        # result = cursor.run_query(optimizal_sqlquery)
                        # lock.release()
                        result = cursor.run_query(optimizal_sqlquery)
                        logging.info("{} 状态已更新至{},uptade result:{}".format(ID, status, result))
                    else:
                        logging.info("lpc={},pid={}位置未变动".format(row[0], row[1]))
                else:
                    logging.info("lpc={},pid={}不存在位置更新数据".format(row[0], row[1]))
        else:
            ID = "pid = {}".format(row[1])
            # judging_condition = "pid"
            logging.info("{} 暂不处理")
            continue
        sub_end_time = time.time() - sub_start_time  # 计算执行耗时
        logging.info(f"本次进程{process_id}操作耗时{sub_end_time}")



def updatebagstatus(process_id):
    offset_value = process_id * 100
    findbagquery = f"""
            select lpc, pid, latest_time, status, flightnr, checked
            FROM (
                select 
                    lpc, 
                    pid, 
                    latest_time, 
                    status, 
                    flightnr, 
                    checked,
                    ROW_NUMBER() OVER (PARTITION BY lpc ORDER BY latest_time DESC) AS rn
                FROM temp_bags
                WHERE 
                    status IS NULL 
                    AND bsm_time >= DATE_ADD(NOW(), INTERVAL - 3 HOUR )
                    AND flighttype = 'D'
                    AND lpc IS NOT NULL
            ) AS subquery
            WHERE rn = 1
            order by pid
            LIMIT 100 offset {offset_value}
            FOR UPDATE SKIP LOCKED;"""
    data = cursor.run_query(findbagquery)
    logging.info("本次更新计划存在{}个行李".format(len(data)))
    querylist = []
    for row in data:
        logging.info(f"进程{process_id}处理{row}")
        sub_start_time = time.time()
        if row[0]:
            ID = "lpc = {}".format(row[0])
            judging_condition = "lpc"

            searchlpcquery = "WITH baginfo AS ( SELECT CURRENTSTATIONID, IDEVENT, pid, lpc FROM WC_PACKAGEINFO WHERE {} ), maxbaginfo AS ( SELECT * FROM baginfo WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM baginfo ) ), trackinginfo AS (  SELECT   IDEVENT,   lpc,   pid,   EVENTTS,   AREAID,   ZONEID,   EQUIPMENTID,   L_DESTINATION,   L_CARRIER,   PROCESSPLANIDNAME   FROM   WC_TRACKINGREPORT track   WHERE   {}   ),  maxtrakinginfo AS ( SELECT * FROM trackinginfo WHERE IDEVENT = ( SELECT max( IDEVENT ) FROM trackinginfo ) ) SELECT  CURRENTSTATIONID,  maxbaginfo.lpc,  maxbaginfo.pid,  EVENTTS,  ( AREAID || '.' || ZONEID || '.' || EQUIPMENTID ) location,  L_DESTINATION,  L_CARRIER,  PROCESSPLANIDNAME  FROM  maxbaginfo  LEFT JOIN maxtrakinginfo ON maxbaginfo.{} = maxtrakinginfo.{}".format(ID, ID, judging_condition, judging_condition)
            baginfo = accessOracle(searchlpcquery)
            for item in baginfo:
                # item格式为CURRENTSTATIONID, lpc, pid, EVENTTS, location, L_DESTINATION, L_CARRIER,flightnr
                # 当packageinfo未找到lpc及flightnr时，要从trackingreport中补充，这两个值只在第一次扫描中输入，二次，三次扫描不更新Lpc和flightnr,先观察2天
                if item[3]:     # 如果有更新记录，则updata，否则无操作
                    localtime = item[3] + datetime.timedelta(hours=8)
                    latest_time = localtime.strftime("%Y-%m-%d %H:%M:%S.%f")
                    # 当行李安检未通过时，有LPC，但行李不会进入系统，可能去开包间，这时WC_TRACKINGREPORT不会有记录，存在有LPC，但无track记录的情况，这时如果status已更新，不需要更新记录
                    # if row[2] != localtime:       # 存在更新的记录,注意row[2]是timestamp，而latest_time是字符串，不能直接比较
                    # 需增加一个到达MCS的目的地
                    if int(item[0]) in [41, 42, 81, 82, 220, 221]:    # 已到达弃包处
                        status = "dump"
                    elif item[0] == item[5]:
                        status = "arrived"
                    elif int(item[0]) in [100, 110, 200, 210]:
                        status = "store"
                    else:
                        status = "unkonwn"
                        logging.info("行李输送中，pid={},CURRENTSTATIONID={},DESTINATION={}".format(item[2], item[0], item[5]))
                    if item[4] in ["12.43.1", "13.43.1", "28.43.1", "29.43.1"]:   # 当最新位置是43.1时，就是去MCS
                        status = "mcs"
                    if item[6]:    # 如果有托盘号，取数字，否则直接取None
                        tubid = int(item[6].split(",")[0][3:])
                    else:
                        tubid = 'Null'
                    # 更正row[4],查看row[4]不存在时是否报错
                    if item[1]:     # 如果lpc不存在baginfo中，,试图获取traking记录
                        lpc = item[1]
                    else:
                        lpc = None
                    if item[7]:     # 如果航班不存在baginfo表记录
                        flightnr = "'{}{}'".format(item[7].split("_")[0], item[7].split("_")[1][0:4])
                    else:
                        flightnr = 'Null'
                    if row[0] and row[4]:      # LPC及flightnr都存在
                        updatebagstatus = "update temp_bags set latest_time = '{}' , current_location='{}',final_destination = '{}', tubid = {}, status = '{}' where {}".format(latest_time, item[4], item[5], tubid, status, ID)
                    elif row[0] and not row[4]:   # LPC存在，flightnr不存在，只更新flightnr
                        updatebagstatus = "update temp_bags set latest_time = '{}', flightnr = {}, current_location='{}',final_destination = '{}', tubid = {}, status = '{}' where {}".format(latest_time, flightnr, item[4], item[5], tubid, status,  ID)
                    elif not row[0] and row[4]:   # LPC不存在，flightnr存在,只更新lpc
                        updatebagstatus = "update temp_bags set latest_time = '{}' ,lpc = {}, current_location='{}',final_destination = '{}', tubid = {}, status = '{}' where {}".format(latest_time, lpc, item[4], item[5], tubid, status,  ID)
                    else:     # LPC及flightnr都不存在
                        updatebagstatus = "update temp_bags set latest_time = '{}' ,lpc = {}, flightnr = {}, current_location='{}',final_destination = '{}', tubid = {}, status = '{}' where {}".format(latest_time, lpc, flightnr, item[4], item[5], tubid, status,  ID)
                    optimizal_sqlquery = updatebagstatus.replace("None", "Null")
                    logging.info("行李位置已更新，这是进程{},语句{}".format(process_id, optimizal_sqlquery))
                    # result = cursor.run_query(optimizal_sqlquery)
                    querylist.append(optimizal_sqlquery)
                else:
                    updatebagstatus = "update temp_bags set status = '{}' where {}".format("notsorted",  ID)             # 更新temp_bags当前位置
                    optimizal_sqlquery = updatebagstatus.replace("None", "Null")
                    logging.info(f"行李位置未更新,{optimizal_sqlquery}")
                    querylist.append(optimizal_sqlquery)
                    # result = cursor.run_query(optimizal_sqlquery)
        else:     # 当lpc不存在时，先不处理这部分
            ID = "pid = {}".format(row[1])
            judging_condition = "pid"
            logging.info("{}暂不处理".format(ID))
            continue
        sub_end_time = time.time() - sub_start_time  # 计算执行耗时
        logging.info(f"本次进程{process_id}操作耗时{sub_end_time}")
    if len(querylist) >= 20:
        logging.info("本次更新{}件行李".format(len(querylist)))
        result = cursor.run_many_query(querylist)
        querylist = []
        
    result = cursor.run_many_query(querylist)



def worker(queue, process_id):
    logging.info(f"Worker 进程 {process_id} 启动")
    while True:
        try:
            start_time = time.time()
            updatebaglocation(process_id)
            cost = time.time() - start_time  # 计算执行耗时
            queue.put({
                "status": "success",
                "process_id": process_id,
                "timestamp": time.time(),
                "cost": cost
            })
        except Exception as e:
            # 向队列中放入失败结果（包含进程ID和异常堆栈）
            queue.put({
                "status": "error",
                "process_id": process_id,
                "message": str(e),
                "traceback": traceback.format_exc(),  # 记录完整堆栈
                "timestamp": time.time()
            })
        # 控制单个进程的执行频率（可选，根据需求调整）
        time.sleep(1)  # 每次检查后休眠1秒（避免空转



if __name__ == '__main__':
    queue = Queue()  # 共享队列（进程间通信）
    processes = []   # 存储所有子进程实例
    for process_id in range(4):
        # time.sleep(process_id)
        p = Process(
            target=worker,
            args=(queue, process_id),  # 传递队列和进程ID
            daemon=True # 设为False（默认），主进程退出时子进程不会强制终止
        )
        p.start()
        processes.append(p)
        logging.info(f"启动 Worker 进程 {process_id}(PID: {p.pid})")
    try:
        # 主进程循环处理结果
        while True:
            # 从队列中获取结果（超时60秒，避免永久阻塞）
            result = queue.get()            
            if result["status"] == "success":
                logging.info(
                    f"检查成功 | 进程 {result['process_id']} | "
                    f"耗时 {result['cost']:.2f}秒 | 时间戳 {result['timestamp']}"
                )
            else:
                logging.error(
                    f"检查失败 | 进程 {result['process_id']} | "
                    f"错误信息: {result['message']} | "
                    f"堆栈: {result['traceback']} | 时间戳 {result['timestamp']}"
                )            
            # 主进程处理完结果后短暂休眠（可选，根据需求调整）
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("用户触发中断（Ctrl+C），准备终止所有子进程...")        
        # 终止所有子进程
        for p in processes:
            if p.is_alive():  # 仅终止仍在运行的进程
                p.terminate()  # 强制终止（可能丢失未写入队列的结果）
                p.join(timeout=2)  # 等待进程退出（超时2秒）
                if p.is_alive():
                    logging.warning(f"进程 {p.pid} 未及时终止，可能需要手动处理")        
        logging.info("所有子进程已终止，主进程退出")