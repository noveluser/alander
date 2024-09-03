#!/usr/bin/python
# coding=utf-8

import oracledb

def execute_query():
    dsn_tns = '10.31.8.21:1521/ORABPI'
    conn = None
    cursor = None
    try:
        conn = oracledb.connect(user='owner_31_bpi_3_0', password='owner31bpi', dsn=dsn_tns)
        cursor = conn.cursor()

        query = """
        WITH TUBINFO AS 
        ( 
        SELECT
            L_CARRIER 
            FROM
                OWNER_31_BPI_3_0.WC_TRACKINGREPORT 
            WHERE
                EVENTTS >= TRUNC( SYSDATE ) 
                AND lpc = {} 
                AND L_CARRIER IS NOT NULL 
            ORDER BY
                EVENTTS
        ),
        BAGINFO AS (
            SELECT
                LPC,
                ( DEPAIRLINE || DEPFLIGHT ) AS flightnr,
                ROW_NUMBER ( ) OVER ( ORDER BY IDEVENT DESC ) AS rn 
            FROM
                WC_PACKAGEINFO 
            WHERE
                LPC = {} 
                AND ROWNUM = 1 
            ORDER BY
                IDEVENT DESC 
            )
        SELECT
            bg.lpc,
            fs.flightnr,
            fs.CLOSE_DT,
            fs.INTIME_ALLOCATED_SORT,
            substr( tubinfo.L_CARRIER, 1, instr( tubinfo.L_CARRIER, ',' ) - 1 ) AS tubid 
        FROM
            FACT_FLIGHT_SUMMARIES_V fs
        JOIN BAGINFO bg ON fs.flightnr = bg.flightnr
        JOIN ( SELECT L_CARRIER FROM TUBINFO WHERE ROWNUM = 1 ) tubinfo ON 1 = 1 
        where 
	        bg.rn = 1 
	        AND FLIGHTDATE = TO_CHAR( SYSDATE, 'YYYY-MM-DD' )
        """.format(3891347891,3891347891)

        # query = """
        # WITH TUBINFO AS 
        # ( 
        # SELECT
        #     L_CARRIER 
        #     FROM
        #         OWNER_31_BPI_3_0.WC_TRACKINGREPORT 
        #     WHERE
        #         EVENTTS >= TRUNC( SYSDATE ) 
        #         AND lpc = {} 
        #         AND L_CARRIER IS NOT NULL 
        #     ORDER BY
        #         EVENTTS
        # ),
        # BAGINFO AS (
        #     SELECT
        #         LPC,
        #         ( DEPAIRLINE || DEPFLIGHT ) AS flightnr,
        #         ROW_NUMBER ( ) OVER ( ORDER BY IDEVENT DESC ) AS rn 
        #     FROM
        #         WC_PACKAGEINFO 
        #     WHERE
        #         LPC = {} 
        #         AND ROWNUM = 1 
        #     ORDER BY
        #         IDEVENT DESC 
        #     )
        # SELECT * 
        # FROM tubINFO
        # """.format(3891347891,3891347891)


        # query = """
        # WITH TUBINFO AS (
        #     SELECT L_CARRIER 
        #     FROM OWNER_31_BPI_3_0.WC_TRACKINGREPORT 
        #     WHERE EVENTTS >= TRUNC(SYSDATE) 
        #       AND LPC = 3891347891 
        #       AND L_CARRIER IS NOT NULL 
        #     ORDER BY EVENTTS
        # ),
        # BAGINFO AS (
        #     SELECT LPC, 
        #            (DEPAIRLINE || DEPFLIGHT) AS flightnr, 
        #            ROW_NUMBER() OVER (ORDER BY IDEVENT DESC) AS rn 
        #     FROM WC_PACKAGEINFO 
        #     WHERE LPC = 3891347891 
        #       AND ROWNUM = 1 
        #     ORDER BY IDEVENT DESC
        # )
        # SELECT bg.LPC, 
        #        fs.flightnr, 
        #        fs.CLOSE_DT, 
        #        fs.INTIME_ALLOCATED_SORT, 
        #        SUBSTR(tubinfo.L_CARRIER, 1, INSTR(tubinfo.L_CARRIER, ',') - 1) AS tubid 
        # FROM FACT_FLIGHT_SUMMARIES_V fs
        # JOIN BAGINFO bg ON fs.flightnr = bg.flightnr
        # JOIN (SELECT L_CARRIER FROM TUBINFO WHERE ROWNUM = 1) tubinfo ON 1 = 1 
        # WHERE bg.rn = 1 
        #   AND fs.FLIGHTDATE = TO_CHAR(SYSDATE, 'YYYY-MM-DD');
        # """

        # query = 'WITH BAGINFO AS ( SELECT LPC FROM WC_PACKAGEINFO WHERE LPC = {}) SELECT * FROM BAGINFO;'.format(3891347891)
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        for row in results:
            print(row)

    except oracledb.DatabaseError as e:
        error, = e.args
        print(f"Oracle error code: {error.code}")
        print(f"Oracle error message: {error.message}")
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

execute_query()