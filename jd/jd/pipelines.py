# -*- coding: utf-8 -*-


import logging

# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
# Define your item pipelines here
#
from scrapy.conf import settings
from scrapy.exceptions import DropItem


class MongoDBPipeline(object):
    def __init__(self):
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
                # key = {}
                # key['sku_id'] = item['sku_id']
                # self.db[item['item_name']].update(key, dict(item), upsert=True)
                self.db[item['item_name']].insert(dict(item))
                logging.debug("add {}".format(item['item_name']))
            except (pymongo.errors.WriteError, KeyError) as err:
                raise DropItem("Duplicated Item: {}".format(item['name']))
        return item
