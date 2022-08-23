#!/usr/bin/python
# coding=utf-8

# 获取当日BHS行李数据
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
                    filename='c://work//log//bhgbag.log',
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
    # 关闭数据库连接
    # cursor.close()
    # db.close()


def count(beforeyesterday, yesterday):
    """统计一天内BHS系统里所有行李记录
    :param items:UTC时间,前天，昨天
    :type itmes:stmap 
    """
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    bhs_bag = conn.cursor()
    sqlquery = "SELECT eventts, lpc, bid, pid, l_DESTINATION FROM OWNER_31_BPI_3_0.WC_TRACKINGREPORT WHERE EVENTTS >= TO_TIMESTAMP( '{} 16:00:00', 'DD-MM-YYYY HH24:MI:SS' ) and EVENTTS < TO_TIMESTAMP( '{} 16:00:00', 'DD-MM-YYYY HH24:MI:SS' ) AND L_CARRIER IS NULL ORDER BY EVENTTS".format(beforeyesterday, yesterday)
    bhs_bag.execute(sqlquery)
    baggage_list = []
    i = 0
    for row in bhs_bag:
        baggage = [row[2], row[3]]
        if baggage not in baggage_list:
            baggage_list.append(baggage)
            eventts = (row[0] + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
            '''过去的将NONE转化成NULL,现在已经用sql = sql.replace("'None'", "NULL").replace("None", "NULL")预计替代
            # if row[1]:
            #     lpc = row[1]
            # else:
            #     lpc = "NULL"
            '''
            sqlquery = "insert into ics.bhsbag (created_time, lpc, bid, pid,destination) values ('{}',{},{},{},{})".format(eventts, row[1], row[2], row[3], row[4])
            writeMysql(sqlquery)      # 写入mysql
            logging.info("{} {} {} {} {}".format(row[0], row[1], row[2], row[3], row[4]))
            i += 1
            """完成BHSbag表的批量写入"""
            if i > 100:
                try:
                    db.commit()
                    # print("finish 100")
                except pymysql.MySQLError as e:
                    logging.error(e)
                i = 0
    conn.close()
    """写入最后少于100部分进入commit"""
    try:
        db.commit()
    except pymysql.MySQLError as e:
        logging.error(e)


def main():
    currentTime = datetime.datetime.now()
    firstdayTime = currentTime - datetime.timedelta(days=2)
    firstday = firstdayTime.strftime("%d-%m-%Y")
    enddayTime = currentTime - datetime.timedelta(days=1)
    endday = enddayTime.strftime("%d-%m-%Y")
    count(firstday, endday)


if __name__ == '__main__':
    main()
