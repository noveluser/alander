#!/usr/bin/python
# coding=utf-8

# 获取当日在线TUB数据
#
# v0.2


import cx_Oracle
import pymysql
import logging
import datetime


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='/mnt/d/temp/temp/test/transfertxt.log',
                    filename='c://work//log//onlinetub.log',
                    # filename='d://temp//temp//test//transfertxt.log',
                    filemode='a')


# 打开数据库连接
# db = pymysql.connect(host='10.110.191.24',
db = pymysql.connect(host='10.31.9.24',
                     user='it',
                     password='1111111',
                     database='ics',
                     charset='utf8mb4')


def writeMysql(sql):
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    sql = sql.replace("'None'", "NULL").replace("None", "NULL")
    try:
        # 执行sql语句
        cursor.execute(sql)
        # 提交到数据库执行
        # db.commit()
        # print("success")
    except Exception as e:
        # Rollback in case there is any error
        db.rollback()
        logging.error(e)
        logging.error(sql)
    # 关闭数据库连接
    # cursor.close()
    # db.close()


def count(beforeyesterday, yesterday):
    nouseTub_list = []
    onetimeTub_list = []
    twotimeTub_list = []
    threetimeTub_list = []
    fourtimeTub_list = []
    fivetimeTub_list = []
    over5timeTub_list = []
    baggage_list = []
    tub_dictionary = dict.fromkeys(range(1, 2281), 0)
    logging.info(tub_dictionary)
    for i in range(25001, 25061):
        tub_dictionary[i] = 0
    """新增3个托盘"""
    tub_dictionary[2314] = 0
    tub_dictionary[2315] = 0
    tub_dictionary[2316] = 0
    OBT_list = []
    SBT_list = []
    departureBag = arriveBag = 0
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    sqlquery = "SELECT eventts, lpc, bid, pid, l_carrier, l_DESTINATION FROM OWNER_31_BPI_3_0.WC_TRACKINGREPORT WHERE EVENTTS >= TO_TIMESTAMP( '{} 16:00:00', 'DD-MM-YYYY HH24:MI:SS' ) and EVENTTS < TO_TIMESTAMP( '{} 16:00:00', 'DD-MM-YYYY HH24:MI:SS' ) AND L_CARRIER IS NOT NULL ORDER BY EVENTTS".format(beforeyesterday, yesterday)
    c.execute(sqlquery)  # use triple quotes if you want to spread your query across multiple lines
    i = 0
    for row in c:
        tubid = int(row[4].split(",")[0][3:])
        baggage = [row[2], row[3], tubid]
        if baggage not in baggage_list:
            localTime = row[0] + datetime.timedelta(hours=8)
            eventts = localTime.strftime("%Y-%m-%d %H:%M:%S")
            baggage_list.append(baggage)
            # # eventts = row[0].strftime("%d-%m-%Y %H:%M:%S")
            # sqlquery = "insert into ics.icsbag (created_time, lpc, bid, pid, l_carrier,destination) values ('{}',{},{},{},{},'{}')".format(eventts, row[1], row[2], row[3], tubid, row[5])
            # writeMysql(sqlquery)      # 写入mysql
            tub_dictionary[tubid] += 1
            if int(tubid) > 20000:
                if tubid not in OBT_list:
                    OBT_list.append(tubid)
            else:
                if tubid not in SBT_list:
                    SBT_list.append(tubid)
            logging.info("{} {} {} {} {} {}".format(row[0], row[1], row[2], row[3], tubid, row[5]))
            destination = int(row[5].split(",")[0][0])
            if destination > 200 and destination < 283:
                departureBag += 1
            else:
                arriveBag += 1
            i += 1
            """完成ICSbag表的批量写入"""
            # if i > 100:
            #     try:
            #         db.commit()
            #         # print("finish 100")
            #     except pymysql.MySQLError as e:
            #         logging.error(e)
            #     i = 0
    conn.close()
    """写入最后少于100部分进入commit"""
    # try:
    #     db.commit()
    # except pymysql.MySQLError as e:
    #     logging.error(e)
    logging.info("{}departure bags count {}, arrived bags count {}, Total {}, use SBT count {}, OBT count{}".format(yesterday, departureBag, arriveBag, len(baggage_list), len(SBT_list), len(OBT_list)))
    for id, number in tub_dictionary.items():
        match number:
            case 0:
                nouseTub_list.append(id)
            case 1:
                onetimeTub_list.append(id)
            case 2:
                twotimeTub_list.append(id)
            case 3:
                threetimeTub_list.append(id)
            case 4:
                fourtimeTub_list.append(id)
            case 5:
                fivetimeTub_list.append(id)
            case _:
                over5timeTub_list.append(id)
        sqlquery = "insert into ics.tubstatus (date, id, usetimes) values ('{}', {}, {}) ".format(yesterday, id, number)
        writeMysql(sqlquery)
    """完成tubstatus表的批量写入"""
    try:
        db.commit()
    except pymysql.MySQLError as e:
        logging.error(e)
    db.close()


def main():
    currentTime = datetime.datetime.now()
    firstdayTime = currentTime - datetime.timedelta(days=2)
    firstday = firstdayTime.strftime("%d-%m-%Y")
    enddayTime = currentTime - datetime.timedelta(days=1)
    endday = enddayTime.strftime("%d-%m-%Y")
    count(firstday, endday)


if __name__ == '__main__':
    main()
