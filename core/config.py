import aredis
from motor import motor_asyncio
from loguru import logger
from urllib.parse import quote_plus

from settings import *

class AioConfig(object):
    """
    配置以及一些方
    """

    def __init__(self, *args, **kwargs):
        self.config_logger()

        self.logger.debug(f"[Connect DB]>> ...")

        self.connect_redis()
        self.connect_mongo()

        self.logger.debug(f"[Init Over]>> ...")


    def config_logger(self):
        """
        配置log
        :return:
        """

        self.logger = logger
        self.logger.add(sink=LOG_PATH, level=LOG_LEVEL, enqueue=True, rotation="200 MB", retention="7 days", colorize=True)

    def connect_redis(self):
        """
        连接redis
        :return:
        """

        self.redis_obj = aredis.StrictRedis(**REDIS_CONN)

        self.logger.debug(f"[redis]>> connect redis {REDIS_CONN['host']}:{REDIS_CONN['port']}/{REDIS_CONN['db']}")

    def connect_mongo(self):
        """
        连接mongo
        :return:
        """

        conn_str = f"mongodb://{MONGO_CONN['username']}:{MONGO_CONN['password']}@{MONGO_CONN['host']}:{MONGO_CONN['port']}"
        self.mongo_obj = motor_asyncio.AsyncIOMotorClient(quote_plus(conn_str))
        self.logger.debug(f"[mongo]>> connect mongo {MONGO_CONN['host']}:{MONGO_CONN['port']}")

