#!/usr/bin/python
# coding=utf-8


import pymongo
import datetime
import logging

logging.basicConfig(
                    level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='d://scheduletask//cleanmongo.log',
                    filemode='a')


if __name__ == '__main__':
    # Connect to MongoDB
    # client = pymongo.MongoClient("mongodb://10.31.8.68:27017/")
    client = pymongo.MongoClient("mongodb://localhost:27017/")

    # Select database and collection
    db = client["IVIMS"]
    col = db["media"]

    # # Delete a record
    # query = { "name": "John" }
    # col.delete_one(query)

    query = {"expirationDate": {"$lt": (datetime.datetime.now() - datetime.timedelta(days=40))}}
    result = col.delete_many(query)
    logging.info("{} documents deleted.".format(result.deleted_count))

    # # Find all documents in the collection
    # cursor = col.find({"_id": "20200917024701204"})
    # for document in cursor:
    #     print(document)
