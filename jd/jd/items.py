# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Field, Item


class ParameterItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    _id = Field()
    item_name = Field()
    sku_id = Field()
    parameters1 = Field()
    parameters2 = Field()
    name = Field()
    price = Field()
    brand = Field()


