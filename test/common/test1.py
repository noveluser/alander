#!/usr/bin/python
# coding=utf-8

# 查找紧急行李
# wangle
# v0.1


import oracledb
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='c://work//log//urgent.log',
    filemode='a'
)


# 定义文件名
filename = 'c://work//log//lpc.txt'


def accessOracle(query, params=None):
    dsn_tns = '10.31.8.21:1521/ORABPI'
    try:
        with oracledb.connect(user='owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns) as conn:
            with conn.cursor() as c:
                c.execute(query, params)
                return c.fetchall()
    except Exception as e:
        logging.error(f"Error occurred while accessing Oracle: {e}")
        return None


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
    UrgencyPackageQuery ="WITH TUBINFO AS ( SELECT L_CARRIER FROM OWNER_31_BPI_3_0.WC_TRACKINGREPORT WHERE EVENTTS >= TRUNC( SYSDATE ) AND lpc = :lpc AND L_CARRIER IS NOT NULL ORDER BY EVENTTS ), BAGINFO AS ( SELECT LPC, ( DEPAIRLINE || DEPFLIGHT ) AS flightnr, ROW_NUMBER ( ) OVER ( ORDER BY IDEVENT DESC ) AS rn  FROM WC_PACKAGEINFO  WHERE LPC = :lpc  AND ROWNUM = 1  ORDER BY IDEVENT DESC  ) SELECT bg.lpc, fs.flightnr, fs.CLOSE_DT, fs.INTIME_ALLOCATED_SORT, substr( tubinfo.L_CARRIER, 1, instr( tubinfo.L_CARRIER, ',' ) - 1 ) AS tubid  FROM FACT_FLIGHT_SUMMARIES_V fs JOIN BAGINFO bg ON fs.flightnr = bg.flightnr JOIN ( SELECT L_CARRIER FROM TUBINFO WHERE ROWNUM = 1 ) tubinfo ON 1 = 1  WHERE bg.rn = 1  AND FLIGHTDATE = TO_CHAR( SYSDATE, 'YYYY-MM-DD' )"
    print(UrgencyPackageQuery)
    return accessOracle(UrgencyPackageQuery, {'lpc': lpc})

    

def main():
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                lpc = int(line.strip())
                bagresult = packageinfo(lpc)
                if bagresult:
                    bagresult = bagresult[0]
                    print(bagresult)
                    logging.info("bag:'%s',flight:'%s','%s','%s','%s'",
                                 bagresult[0],
                                 bagresult[1],
                                 bagresult[2].strftime('%Y-%m-%d %H:%M:%S'),
                                 bagresult[3],
                                 bagresult[4])
    except FileNotFoundError:
        logging.error(f"文件 {filename} 未找到。")
    except Exception as e:
        logging.error(f"发生错误: {e}")



if __name__ == '__main__':
    main()