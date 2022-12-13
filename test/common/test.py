#!/usr/bin/python
# coding=utf-8


# 导入需要的库
from my_mysql import Database
import time

# envioments
# pool = Database(pool_size=10, host='10.110.191.24', user='it', password='1111111', database='ics', port=3306)


if __name__ == "__main__":
    # findbagquery = "SELECT * FROM temp_bags LIMIT 1"
    # for i in range(5):
    #     result = pool.run_query(findbagquery)
    #     print(result)
    # pool.close()
    with Database(pool_size=10, host='10.110.191.24', user='it', password='1111111', database='ics', port=3306) as pool:
        findbagquery = "SELECT * FROM temp_bags LIMIT 1"
        for i in range(5):
            result = pool.run_query(findbagquery)
            print(result)




