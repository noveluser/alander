#!/usr/bin/python
# coding=utf-8


# 导入需要的库
import pymysql


# envornment


class MySQL:
    def __init__(self, host, username, password, dbname):
        self.host = host
        self.user = username
        self.password = password
        self.db = dbname

    def insert_batch(self, data):
        try:
            connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                db=self.db,
                cursorclass=pymysql.cursors.DictCursor
            )

            cursor = connection.cursor()

            # Use a for loop to insert many rows
            ID = 'lpc = 89058766'
            for row in data:
                sql = "update temp_bags set latest_time = '{}' ,lpc = {}, flightnr = {}, current_location='{}',final_destination = '{}', tubid = {}, status = '{}', checked = {} where {}".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], ID)
                # cursor.execute("INSERT INTO " + table + " (field1, field2, field3) VALUES (%s, %s, %s)", row)
                cursor.execute(sql)
            connection.commit()
            print(cursor.rowcount, "Record inserted successfully into table")

        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            if (connection.ping()):
                cursor.close()
                connection.close()
                print("MySQL connection is closed")


if __name__ == '__main__':
    cursor = MySQL(dbname='ics', username='it', password='1111111', host='10.31.9.24')
    ID = 'lpc = 89058766'
    data = [('2000-01-28 09:32:30.219000', 89058766, '\'ZH0000\'', '3133.7.2', '217', 178, 'arrived', 'Null')]
    cursor.insert_batch(data)
