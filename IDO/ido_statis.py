#!/usr/bin/python
# coding=utf-8

# 获取当日指定区的IDO统计数据
# 避免BPI数据库清理
# v0.2


import cx_Oracle
import logging
import datetime
from my_mysql import Database


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/data/package/crontab/log/idoStatis.log',
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


def main():

    zone = [3101, 3104, 3107, 3108, 3201, 3202, 3203, 3204, 3241, 3242, 3243, 3244, 3143, 3144, 3111, 3113, 3114, 3119, 3120, 3149, 3150]
    for area in zone:
        # anothercursor = Database(dbname='ics', username='it', password='1111111', host='10.31.9.24', port='3306')
        ido_list = cursor.run_query("select areaid, zoneid, equipmentid from ics.ido_list where areaid ={}".format(area))
        for ido in ido_list:
            sqlquery = "SELECT EVENTTS, AREAID,ZONEID,EQUIPMENTID,STATISTICALID,VALUE FROM  WC_STATISTICALDATA  WHERE AREAID = {} and zoneid = {} and EQUIPMENTID = {} and EVENTTS  > TRUNC(SYSDATE-1) and EVENTTS  < TRUNC(SYSDATE) ".format(ido[0], ido[1], ido[2])
            data = accessOracle(sqlquery)
            for row in data:
                orignal_sqlquery = "insert into ics.ido_statis (EVENTTS, AREAID,ZONEID,EQUIPMENTID,STATISTICALID,VALUE)  values ('{}',{},{},{},{},{})".format((row[0]+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S.%f"), row[1], row[2], row[3], row[4], row[5])
                optimizal_sqlquery = orignal_sqlquery.replace("'None'", "Null")
                logging.info(optimizal_sqlquery)
                cursor.run_query(optimizal_sqlquery)


if __name__ == '__main__':
    main()
