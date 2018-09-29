#!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# author:Samray <samrayleung@gmail.com>
import json
import logging
import uuid

import scrapy
from jd_comment.db import init_mongodb
from jd_comment.items import CommentItem, CommentSummaryItem
from scrapy_redis.spiders import RedisSpider


class JDCommentSpider(RedisSpider):
    name = 'jd_comment'

    def __init__(self):
        self.comment_url = "https://sclub.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98vv118416&productId={}&score=0&sortType=5&page={}&pageSize=10"
        self.db = init_mongodb()
        self.item_db_parameter_name = "parameter"

    def start_requests(self):
        goods = self.get_item_sku_id()
        for sku_id, good_name in goods:
            yield scrapy.Request(url=self.comment_url.format(sku_id, 1),
                                 callback=self.parse_comment,
                                 meta={'page': 1,
                                       'sku_id': sku_id,
                                       'good_name': good_name})

    def parse_comment(self, response):
        comment_item = CommentItem()
        page = response.meta['page']
        sku_id = response.meta['sku_id']
        good_name = response.meta['good_name']
        content = response.text
        # 可能评论为空
        if content != '':
            content = content.replace(r"fetchJSON_comment98vv118416(", "")
            content = content.replace(r");", "")
            data = json.loads(content)
            max_page = data.get("maxPage")
            comments = data.get("comments")
            comment_item['good_name'] = good_name
            comment_item['item_name'] = 'comment'
            comment_item['_id'] = str(uuid.uuid4())
            for comment in comments:
                comment_item['sku_id'] = sku_id
                comment_item['comment_id'] = comment.get('id')  # 评论的 id
                comment_item['content'] = comment.get('content')  # 评论的内容
                comment_item['creation_time'] = comment.get(
                    'creationTime', '')  # 评论创建的时间
                comment_item['reply_count'] = comment.get(
                    'replyCount', '')  # 回复数量

                comment_item['score'] = comment.get('score', '')  # 评星
                comment_item['useful_vote_count'] = comment.get(
                    'usefulVoteCount', '')  # 其他用户觉得有用的数量
                comment_item['useless_vote_count'] = comment.get(
                    'uselessVoteCount', '')  # 其他用户觉得无用的数量
                comment_item['user_level_id'] = comment.get(
                    'userLevelId', '')  # 评论用户等级的 id
                comment_item['user_province'] = comment.get(
                    'userProvince', '')  # 用户的省份
                comment_item['nickname'] = comment.get(
                    'nickname', '')  # 评论用户的昵称
                comment_item['user_level_name'] = comment.get(
                    'userLevelName', '')  # 评论用户的等级
                comment_item['user_client'] = comment.get(
                    'userClient', '')  # 用户评价平台
                comment_item['user_client_show'] = comment.get(
                    'userClientShow', '')  # 用户评价平台
                comment_item['is_mobile'] = comment.get(
                    'isMobile', '')  # 是否是在移动端完成的评价
                comment_item['days'] = comment.get('days', '')  # 购买后评论的天数
                comment_item['reference_time'] = comment.get(
                    'referenceTime', '')  # 购买的时间
                comment_item['after_days'] = comment.get(
                    'afterDays', '')  # 购买后再次评论的天数
                after_user_comment = comment.get('afterUserComment', '')
                if after_user_comment != '' and after_user_comment is not None:
                    h_after_user_comment = after_user_comment.get(
                        'hAfterUserComment', '')
                    after_content = h_after_user_comment.get(
                        'content', '')  # 再次评论的内容
                    comment_item['after_user_comment'] = after_content
                yield comment_item
            if page < max_page:
                yield scrapy.Request(url=self.comment_url.format(sku_id, page + 1),
                                     callback=self.parse_comment,
                                     meta={'page': page + 1, 'sku_id': sku_id,
                                           'good_name': good_name
                                           })
            elif page >= max_page:
                summary_item = CommentSummaryItem()
                comment_summary = data.get("productCommentSummary")
                summary_item['item_name'] = 'comment_summary'
                summary_item['poor_rate'] = comment_summary.get('poorRate')
                summary_item['good_rate'] = comment_summary.get('goodRate')
                summary_item['good_count'] = comment_summary.get('goodCount')
                summary_item['general_count'] = comment_summary.get(
                    'generalCount')
                summary_item['poor_count'] = comment_summary.get('poorCount')
                summary_item['after_count'] = comment_summary.get('afterCount')
                summary_item['average_score'] = comment_summary.get(
                    'averageScore')
                summary_item['sku_id'] = sku_id
                summary_item['good_name'] = good_name
                summary_item['_id'] = str(uuid.uuid4())
                yield summary_item

    def parse_comment_json(self, content):
        data = json.loads(content)
        comments = data.get("comments")
        for comment in comments:
            item = CommentItem()
            item['comment_id'] = comment.get('id')  # 评论的 id
            item['content'] = comment.get('content')  # 评论的内容
            item['creation_time'] = comment.get(
                'creationTime', '')  # 评论创建的时间
            item['reply_count'] = comment.get('replyCount', '')  # 回复数量

            item['score'] = comment.get('score', '')  # 评星
            item['useful_vote_count'] = comment.get(
                'usefulVoteCount', '')  # 其他用户觉得有用的数量
            item['useless_vote_count'] = comment.get(
                'uselessVoteCount', '')  # 其他用户觉得无用的数量
            item['user_level_id'] = comment.get(
                'userLevelId', '')  # 评论用户等级的 id
            item['user_province'] = comment.get('userProvince', '')  # 用户的省份
            item['nickname'] = comment.get('nickname', '')  # 评论用户的昵称
            item['user_level_name'] = comment.get(
                'userLevelName', '')  # 评论用户的等级
            item['user_client'] = comment.get('userClient', '')  # 用户评价平台
            item['user_client_show'] = comment.get(
                'userClientShow', '')  # 用户评价平台
            item['is_mobile'] = comment.get('isMobile', '')  # 是否是在移动端完成的评价
            item['days'] = comment.get('days', '')  # 购买后评论的天数
            item['reference_time'] = comment.get(
                'referenceTime', '')  # 购买的时间
            item['after_days'] = comment.get('afterDays', '')  # 购买后再次评论的天数
            after_user_comment = comment.get('afterUserComment', '')
            if after_user_comment != '' and after_user_comment is not None:
                h_after_user_comment = after_user_comment.get(
                    'hAfterUserComment', '')
                after_content = h_after_user_comment.get(
                    'content', '')  # 再次评论的内容
                item['after_user_comment'] = after_content
                yield item

    def get_item_sku_id(self):
        item_parameters = self.db[self.item_db_parameter_name].find({})
        parameters = [parameter for parameter in item_parameters]
        sku_ids = []
        good_names = []
        for parameter in parameters:
            sku_id = dict(parameter).get("sku_id", None)
            good_name = dict(parameter).get("name", None)
            if good_name:
                good_name = good_name[0]
            else:
                good_name = ''
            sku_ids.append(sku_id)
            good_names.append(good_name)
        good = set(list(zip(sku_ids, good_names)))
        return list(good)
