# from scrapy import log
import logging
import pprint
import socket
from time import time

import redis
# from scrapy.statscol import StatsCollector
from scrapy.statscollectors import StatsCollector

# from jd.utils import color
from jd_comment.utils import color

# default values
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
STATS_KEY = 'scrapy:jd_comment:stats'


class GraphiteClient(object):
    """
        The client thats send data to graphite.

        Can have some ideas from /opt/graphite/examples/example-client.py
    """

    def __init__(self, host="127.0.0.1", port=2003):
        self.style = color.color_style()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))

    def send(self, metric, value, timestamp=None):
        try:
            message = "{} {} {}\n\n".format(
                metric, value, timestamp or int(time()))
            self._sock.sendall(message.encode())
        except Exception as err:
            self.style.ERROR("SocketError(GraphiteClient): " + str(err))


class GraphiteStatsCollector(StatsCollector):
    """
        send the stats data to graphite.
    """

    GRAPHITE_HOST = '127.0.0.1'
    GRAPHITE_PORT = 2003
    GRAPHITE_IGNOREKEYS = []  # to ignore it,prevent to send data to graphite

    def __init__(self, crawler):
        super(GraphiteStatsCollector, self).__init__(crawler)

        host = crawler.settings.get("GRAPHITE_HOST", self.GRAPHITE_HOST)
        port = crawler.settings.get("GRAPHITE_PORT", self.GRAPHITE_PORT)
        self.ignore_keys = crawler.settings.get(
            "GRAPHITE_IGNOREKEYS", self.GRAPHITE_IGNOREKEYS)
        self._graphiteclient = GraphiteClient(host, port)

    def _get_stats_key(self, spider, key):
        if spider is not None:
            return "scrapy.spider.%s.%s" % (spider.name, key)
        return "scrapy.%s" % (key)

    def set_value(self, key, value, spider=None):
        super(GraphiteStatsCollector, self).set_value(key, value, spider)
        self._set_value(key, value, spider)

    def _set_value(self, key, value, spider):
        if isinstance(value, (int, float)) and key not in self.ignore_keys:
            k = self._get_stats_key(spider, key)
            self._graphiteclient.send(k, value)

    def inc_value(self, key, count=1, start=0, spider=None):
        super(GraphiteStatsCollector, self).inc_value(
            key, count, start, spider)
        self._graphiteclient.send(self._get_stats_key(
            spider, key), self.get_value(key))

    def max_value(self, key, value, spider=None):
        super(GraphiteStatsCollector, self).max_value(key, value, spider)
        self._graphiteclient.send(self._get_stats_key(
            spider, key), self.get_value(key))

    def min_value(self, key, value, spider=None):
        super(GraphiteStatsCollector, self).min_value(key, value, spider)
        self._graphiteclient.send(self._get_stats_key(
            spider, key), self.get_value(key))

    def set_stats(self, stats, spider=None):
        super(GraphiteStatsCollector, self).set_stats(stats, spider)
        for key in stats:
            self._set_value(key, stats[key], spider)


class RedisStatsCollector(object):
    """
        Save stats data in redis for distribute situation.
    """

    def __init__(self, crawler):
        self._dump = crawler.settings.getbool(
            'STATS_DUMP')  # default: STATS_DUMP = True
        host = crawler.settings.get('REDIS_HOST', REDIS_HOST)
        port = crawler.settings.get('REDIS_PORT', REDIS_PORT)
        self.stats_key = crawler.settings.get('STATS_KEY', STATS_KEY)
        self.server = redis.Redis(host, port)

    def get_value(self, key, default=None, spider=None):
        if self.server.hexists(self.stats_key, key):
            return int(self.server.hget(self.stats_key, key))
        else:
            return default

    def get_stats(self, spider=None):
        return self.server.hgetall(self.stats_key)

    def set_value(self, key, value, spider=None):
        self.server.hset(self.stats_key, key, value)

    def set_stats(self, stats, spider=None):
        self.server.hmset(self.stats_key, stats)

    def inc_value(self, key, count=1, start=0, spider=None):
        if not self.server.hexists(self.stats_key, key):
            self.set_value(key, start)
        self.server.hincrby(self.stats_key, key, count)

    def max_value(self, key, value, spider=None):
        self.set_value(key, max(self.get_value(key, value), value))

    def min_value(self, key, value, spider=None):
        self.set_value(key, min(self.get_value(key, value), value))

    def clear_stats(self, spider=None):
        self.server.delete(self.stats_key)

    def open_spider(self, spider):
        pass

    def close_spider(self, spider, reason):
        if self._dump:
            logging.debug("Dumping Scrapy stats:\n" + pprint.pformat(self.get_stats()),
                          spider=spider)
        self._persist_stats(self.get_stats(), spider)

    def _persist_stats(self, stats, spider):
        pass


class RedisGraphiteStatsCollector(RedisStatsCollector):
    """
        send the stats data to graphite and save stats data in redis for distribute situation.

    """

    GRAPHITE_HOST = '127.0.0.1'
    GRAPHITE_PORT = 2003
    GRAPHITE_IGNOREKEYS = []  # to ignore it,prevent to send data to graphite

    def __init__(self, crawler):
        super(RedisGraphiteStatsCollector, self).__init__(crawler)
        host = crawler.settings.get("GRAPHITE_HOST", self.GRAPHITE_HOST)
        port = crawler.settings.get("GRAPHITE_PORT", self.GRAPHITE_PORT)
        self.ignore_keys = crawler.settings.get(
            "GRAPHITE_IGNOREKEYS", self.GRAPHITE_IGNOREKEYS)
        self._graphiteclient = GraphiteClient(host, port)

    def _get_stats_key(self, spider, key):
        if spider is not None:
            return "scrapy.spider.%s.%s" % (spider.name, key)
        return "scrapy.%s" % (key)

    def set_value(self, key, value, spider=None):
        super(RedisGraphiteStatsCollector, self).set_value(key, value, spider)
        self._set_value(key, value, spider)

    def _set_value(self, key, value, spider):
        if isinstance(value, (int, float)) and key not in self.ignore_keys:
            k = self._get_stats_key(spider, key)
            self._graphiteclient.send(k, value)

    def inc_value(self, key, count=1, start=0, spider=None):
        super(RedisGraphiteStatsCollector, self).inc_value(
            key, count, start, spider)
        self._graphiteclient.send(self._get_stats_key(
            spider, key), self.get_value(key))

    def max_value(self, key, value, spider=None):
        super(RedisGraphiteStatsCollector, self).max_value(key, value, spider)
        self._graphiteclient.send(self._get_stats_key(
            spider, key), self.get_value(key))

    def min_value(self, key, value, spider=None):
        super(RedisGraphiteStatsCollector, self).min_value(key, value, spider)
        self._graphiteclient.send(self._get_stats_key(
            spider, key), self.get_value(key))

    def set_stats(self, stats, spider=None):
        super(RedisGraphiteStatsCollector, self).set_stats(stats, spider)
        for key in stats:
            self._set_value(key, stats[key], spider)
