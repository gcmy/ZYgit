#!/usr/bin/python
# -*- coding: utf-8 -*-

from pymongo import MongoClient
from pymongo import UpdateOne
from configparser import ConfigParser

class ToolMongoClient(object):
    def __init__(self, cfg_fn):
        self._mongod_host = None
        self._mongod_port = None
        self._mongodb_user = None
        self._mongodb_passwd = None
        self._mongodb_db_str = None

        self.__read_configure(cfg_fn)

        self._mongo_client = MongoClient(self._mongod_host, self._mongod_port)
        self._mongo_client[self._mongodb_db_str].authenticate(self._mongodb_user, self._mongodb_passwd)
        self._mongo_db = self._mongo_client[self._mongodb_db_str]

    def __read_configure(self, cfg_fn="mongod.cfg"):
        cf = ConfigParser()
        cf.read(filenames=cfg_fn)

        # mongod配置
        self._mongod_host = cf.get("mongod", "mongod_host")
        self._mongod_port = cf.getint("mongod", "mongod_port")
        self._mongodb_user = cf.get("mongod", "mongodb_user")
        self._mongodb_passwd = cf.get("mongod", "mongodb_passwd")
        self._mongodb_db_str = cf.get("mongod", "mongodb_db")

    def save_record(self, record, collection_name):
        self._mongo_db[collection_name].save(record, manipulate=False)

    def save_records(self, records, collection_name):
        ops = []
        for record in records:
            ops.append(UpdateOne({"_id": record.get("_id")}, {'$set':record}, True))
        self._mongo_db[collection_name].bulk_write(ops)

    def find_one(self, collection_name, filter=None, projection=None, sort=None):
        return self._mongo_db[collection_name].find_one(filter=filter, projection=projection, sort=sort)

    def find_many(self, collection_name, filter=None, projection=None, sort=None, limit=None):
        if limit:
            return self._mongo_db[collection_name].find(filter=filter, projection=projection, sort=sort, no_cursor_timeout=True).limit(limit)
        else:
            return self._mongo_db[collection_name].find(filter=filter, projection=projection, sort=sort, no_cursor_timeout=True)

    def ensure_index(self, collection_name, key_tuple_list):
        self._mongo_db[collection_name].ensure_index(key_or_list=key_tuple_list)

    def update_many(self, collection_name, filter, update):
        self._mongo_db[collection_name].update_many(filter=filter, update=update)