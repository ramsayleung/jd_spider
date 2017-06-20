# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Field, Item


class CommentItem(Item):
    sku_id = Field()
    _id = Field()                 # id
    good_name = Field()
    item_name = Field()
    comment_id = Field()        # 评论 ID
    content = Field()           # 评论内容
    creation_time = Field()       # 评论创建的时间
    reply_count = Field()       # 回复的数量
    score = Field()             # 评分
    useful_vote_count = Field()  # 觉得该评论有用的投票
    useless_vote_count = Field()  # 觉得该评论无用的投票
    user_level_id = Field()       # 用户等级 id
    user_province = Field()       # 用户所在省份
    nickname = Field()           # 用户昵称
    user_level_name = Field()     # 用户等级名
    user_client = Field()         # 用户评论的平台的类型
    user_client_show = Field()    # 用户评论的平台的名称
    is_mobile = Field()           # 是否是来自移动端
    days = Field()                # 是否
    reference_time = Field()
    after_days = Field()
    after_user_comment = Field()


class CommentSummaryItem(Item):
    sku_id = Field()
    _id = Field()
    item_name = Field()
    content = Field()
    good_name = Field()   # 商品的名称
    average_score = Field()       # 平均得分
    good_count = Field()          # 好评数
    general_count = Field()       # 中评数
    poor_count = Field()          # 差评数
    after_count = Field()         # 追评数
    good_rate = Field()           # 好评率
    general_rate = Field()        # 中评率
    poor_rate = Field()           # 差评率
