# -*- coding: utf-8 -*-

import logging

import pymongo
# Define your item pipelines here
#
from scrapy.conf import settings
from scrapy.exceptions import DropItem

from jd_comment.db import init_mongodb


# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class MongoDBPipeline(object):
    def __init__(self):
        self.db = init_mongodb()

    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            try:
                key = {}
                self.db[item['item_name']].insert(dict(item))
                logging.debug("add {}".format(item['item_name']))
            except (pymongo.errors.WriteError, KeyError) as err:
                raise DropItem(
                    "Duplicated comment Item: {}".format(item['good_name']))
        return item
