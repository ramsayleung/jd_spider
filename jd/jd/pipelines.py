# -*- coding: utf-8 -*-


import logging
import os

# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import redis
# Define your item pipelines here
#
from scrapy.conf import settings
from scrapy.exceptions import DropItem


class MongoDBPipeline(object):
    def __init__(self):
        pool = redis.ConnectionPool(
            host=settings['REDIS_HOST'], port=settings['REDIS_PORT'], db=0)
        self.server = redis.Redis(connection_pool=pool)
        connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        self.db = connection[settings['MONGODB_DB']]

    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            try:
                # This returns the number of values added, zero if already exists.
                key = "good_filter_" + item['sku_id']
                item_name = item['item_name']
                if self.server.get(key) is None:
                    self.server.set(key, item_name)
                    self.db[item_name].insert(dict(item))
                    logging.debug("add {}".format(item['item_name']))
            except (pymongo.errors.WriteError, KeyError) as err:
                raise DropItem("Duplicated Item: {}".format(item['name']))
        return item
