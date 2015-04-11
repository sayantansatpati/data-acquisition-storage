#!/usr/bin/env python

__author__ = 'ssatpati'

import sys

import pymongo
from pymongo import MongoClient

from config import Config


class Mongo(object):

    def __init__(self, db_name):
        self.config = Config()
        # Init MongoDB Client & DB
        self.client = MongoClient(self.__get_config_val("uri"))
        self.db = self.client[db_name]

    def insert(self, data, collection_name="tweets"):
        coll = self.db[collection_name]
        try:
            coll.insert(data)
        except pymongo.errors.DuplicateKeyError as dk:
            print("### Exception inserting data: ", dk)
        except:
            e = sys.exc_info()[0]
            print(e)
            raise

    def collection(self, collection_name="tweets"):
        return self.db[collection_name]

    def get_batch_size(self):
        return self.__get_config_val("batch_size")

    def __get_config_val(self, key):
        return self.config.get("mongo", key)

