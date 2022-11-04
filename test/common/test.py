#!/usr/bin/python
# coding=utf-8


row = [None, 1,2,3,4,5]
orignal_sqlquery = "insert into ics.flight (create_time, flightnr, std, ARRIVALORDEPARTURE, INTERNATIONALORDOMESTIC, HANDLER)  values ('{}','{}','{}','{}','{}','{}')".format(row[0], row[1], row[2], row[3], row[4], row[5])
optimizal_sqlquery = orignal_sqlquery.replace("'None'", "Null")
print(optimizal_sqlquery)