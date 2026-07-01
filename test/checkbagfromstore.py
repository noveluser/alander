#!/usr/bin/python
# coding=utf-8

import oracledb
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='c://work//log//1.log',
    filemode='a'
)

def accessOracle(query, params=None):
    dsn_tns = oracledb.makedsn('10.31.8.21', '1521', service_name='ORABPI')
    conn = oracledb.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)
    c = conn.cursor()
    c.execute(query, params) if params else c.execute(query)
    result = c.fetchall()
    conn.close()
    return result

def searchbagfrommcs(yesterday):
    dt = datetime.strptime(yesterday, '%Y-%m-%d')
    start_ts = dt.strftime('%d-%m-%Y') + ' 00:00:00'
    end_ts = (dt + timedelta(days=1)).strftime('%d-%m-%Y') + ' 00:00:00'
    
    query = """
        SELECT DISTINCT lpc
        FROM FACT_BAG_SUMMARIES_V
        WHERE MCS_RECOGNITION IS NOT NULL
          AND MANUAL_SCAN_LOCATION = 'MCS03'
          AND REGISTER_DT > TO_TIMESTAMP(:start_ts, 'DD-MM-YYYY HH24:MI:SS')
          AND REGISTER_DT < TO_TIMESTAMP(:end_ts, 'DD-MM-YYYY HH24:MI:SS')
    """
    data = accessOracle(query, {'start_ts': start_ts, 'end_ts': end_ts})
    logging.info("从MCS表中查询到 {} 个唯一LPC".format(len(data)))
    return data

def judgebag(lpn, yesterday):
    base = datetime.strptime(yesterday, '%Y-%m-%d')
    event_start = base - timedelta(hours=8)
    event_end = base + timedelta(hours=16)
    
    query = """
        SELECT IDEVENT, EXECUTEDTASK
        FROM (
            SELECT IDEVENT, EXECUTEDTASK,
                   ROW_NUMBER() OVER (PARTITION BY EXECUTEDTASK ORDER BY IDEVENT DESC) AS rn
            FROM WC_PACKAGEINFO
            WHERE EVENTTS >= :event_start
              AND EVENTTS < :event_end 
              AND TARGETPROCESSID LIKE 'BSIS%'
              AND LPC = :lpn
              AND EXECUTEDTASK IN ('ManualScan', 'Store')   
        )
        WHERE rn = 1
        ORDER BY EXECUTEDTASK
    """
    data = accessOracle(query, {'lpn': lpn, 'event_start': event_start, 'event_end': event_end})
    if data:
        logging.info("LPC {} 查询结果: {}".format(lpn, data))
    else:
        logging.info("LPC {} 未查询到符合条件的记录".format(lpn))
    return data

if __name__ == '__main__':
    # 自动获取昨天的日期，也可手动指定：yesterday = "2026-06-30"
    # yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    yesterday = "2026-06-30"
    lpns = searchbagfrommcs(yesterday)
    to_mcs = []
    for lpn_tuple in lpns:
        lpn = lpn_tuple[0]
        result = judgebag(lpn, yesterday)
        if len(result) == 2 and result[1][0] < result[0][0]:
            to_mcs.append(lpn)
    
    logging.info("有{}件行李来自早到".format(len(to_mcs)))
    logging.info(to_mcs)