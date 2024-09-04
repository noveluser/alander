#!/usr/bin/python
# coding=utf-8

# 查找紧急行李
# wangle
# v0.1


import cx_Oracle
import logging


logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='test.log',
                    filemode='a')


# 定义文件名
filename = 'lpc.txt'


def accessOracle(query):
    dsn_tns = cx_Oracle.makedsn('10.31.8.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    # dsn_tns = cx_Oracle.makedsn('10.110.190.21', '1521', service_name='ORABPI')  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)  # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    c = conn.cursor()
    try:
        c.execute(query)  # use triple quotes if you want to spread your query across multiple lines
        result = c.fetchall()
    except Exception as e:
        logging.error(e)
        result = []
    finally:
        conn.close()
    return result


def packageinfo(lpc):
    # UrgencyPackageQuery = """
    #     WITH TUBINFO AS 
    #     ( 
    #     SELECT
    #         L_CARRIER 
    #         FROM
    #             OWNER_31_BPI_3_0.WC_TRACKINGREPORT 
    #         WHERE
    #             EVENTTS >= TRUNC( SYSDATE ) 
    #             AND lpc = {} 
    #             AND L_CARRIER IS NOT NULL 
    #         ORDER BY
    #             EVENTTS
    #     ),
    #     BAGINFO AS (
    #         SELECT
    #             LPC,
    #             ( DEPAIRLINE || DEPFLIGHT ) AS flightnr,
    #             ROW_NUMBER ( ) OVER ( ORDER BY IDEVENT DESC ) AS rn 
    #         FROM
    #             WC_PACKAGEINFO 
    #         WHERE
    #             LPC = {} 
    #             AND ROWNUM = 1 
    #         ORDER BY
    #             IDEVENT DESC 
    #         )
    #     SELECT
    #         bg.lpc,
    #         fs.flightnr,
    #         fs.CLOSE_DT,
    #         fs.INTIME_ALLOCATED_SORT,
    #         substr( tubinfo.L_CARRIER, 1, instr( tubinfo.L_CARRIER, ',' ) - 1 ) AS tubid 
    #     FROM
    #         FACT_FLIGHT_SUMMARIES_V fs
    #     JOIN BAGINFO bg ON fs.flightnr = bg.flightnr
    #     JOIN ( SELECT L_CARRIER FROM TUBINFO WHERE ROWNUM = 1 ) tubinfo ON 1 = 1 
    #     where 
	#         bg.rn = 1 
	#         AND FLIGHTDATE = TO_CHAR( SYSDATE, 'YYYY-MM-DD' )
    #     """.format(lpc, lpc)
    UrgencyPackageQuery ="WITH TUBINFO AS ( SELECT L_CARRIER FROM OWNER_31_BPI_3_0.WC_TRACKINGREPORT WHERE EVENTTS >= TRUNC( SYSDATE ) AND lpc = {} AND L_CARRIER IS NOT NULL ORDER BY EVENTTS ), BAGINFO AS ( SELECT LPC, ( DEPAIRLINE || DEPFLIGHT ) AS flightnr, ROW_NUMBER ( ) OVER ( ORDER BY IDEVENT DESC ) AS rn  FROM WC_PACKAGEINFO  WHERE LPC = {}  AND ROWNUM = 1  ORDER BY IDEVENT DESC  ) SELECT bg.lpc, fs.flightnr, fs.CLOSE_DT, fs.INTIME_ALLOCATED_SORT, substr( tubinfo.L_CARRIER, 1, instr( tubinfo.L_CARRIER, ',' ) - 1 ) AS tubid  FROM FACT_FLIGHT_SUMMARIES_V fs JOIN BAGINFO bg ON fs.flightnr = bg.flightnr JOIN ( SELECT L_CARRIER FROM TUBINFO WHERE ROWNUM = 1 ) tubinfo ON 1 = 1  WHERE bg.rn = 1  AND FLIGHTDATE = TO_CHAR( SYSDATE, 'YYYY-MM-DD' )".format(lpc, lpc)
    # UrgencyPackageQuery ="select lpc from  WC_PACKAGEINFO where lpc = 3781561182" 
    return accessOracle(UrgencyPackageQuery)
    # return accessOracle(UrgencyPackageQuery, {'lpc': lpc})

    

def main():
    try:
        # 打开文件并读取每一行
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                lpc = int(line.strip())  # 去除行尾的换行符
                bagresult = packageinfo(lpc)[0]
                print(bagresult)
                # logging.info("'{}','{}','{}','{}','{}'".format(bagresult[0],bagresult[1],bagresult[2].strftime('%Y-%m-%d %H:%M:%S'),bagresult[3],bagresult[4]))
    except FileNotFoundError:
        print(f"文件 {filename} 未找到。")
    except Exception as e:
        print(f"发生错误: {e}")



if __name__ == '__main__':
    main()