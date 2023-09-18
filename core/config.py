import aioredis
from motor import motor_asyncio
from loguru import logger
from urllib.parse import quote_plus

from settings import *
from utils.bloom_filter import AioBloomFilter

class AioConfig(object):
    """
    配置以及一些方
    """

    def __init__(self, *args, **kwargs):
        self.config_logger()

        self.logger.debug(f"[Connect DB]>> ...")

        self.connect_redis()
        self.connect_mongo()


        # 入参 初始化配置
        self.is_dup = kwargs.get('is_dup', False)  # 是否去重 采用redis实现布隆过滤器

        if self.is_dup:
            self.config_bloom_filter()


        self.logger.debug(f"[Init Over]>> ...")


    def config_logger(self):
        """
        配置log
        :return:
        """

        self.logger = logger
        self.logger.add(sink=LOG_PATH, level=LOG_LEVEL, enqueue=True, rotation="200 MB", retention="7 days", colorize=True)


    def config_bloom_filter(self):
        """
        配置异步布隆过滤器
        :return:
        """
        key = f"{self.__class__.__name__}:BloomFilter"
        self.filter = AioBloomFilter(conn_obj=self.redis_obj, key=key)
        self.logger.debug(f"[bloomfilter]>> {key}: \n总bit位数: {self.filter.m}\n最少hash次数: {self.filter.k}\n需要内存(M): {self.filter.mem}\n需要内存块: {self.filter.blocknum}\n随机种子: {self.filter.seeds}")

    def connect_redis(self):
        """
        连接redis
        :return:
        """

        self.redis_obj = aioredis.from_url(f"redis://{REDIS_CONN['host']}", port=REDIS_CONN['port'], password=REDIS_CONN['password'], db=REDIS_CONN['db'], encoding="utf-8", decode_responses=True)
        self.logger.debug(f"[redis]>> connect redis {REDIS_CONN['host']}:{REDIS_CONN['port']}/{REDIS_CONN['db']}")

    def connect_mongo(self):
        """
        连接mongo
        :return:
        """

        conn_str = f"mongodb://{MONGO_CONN['username']}:{quote_plus(MONGO_CONN['password'])}@{MONGO_CONN['host']}:{MONGO_CONN['port']}"
        self.mongo_obj = motor_asyncio.AsyncIOMotorClient(conn_str)
        self.logger.debug(f"[mongo]>> connect mongo {MONGO_CONN['host']}:{MONGO_CONN['port']}")
