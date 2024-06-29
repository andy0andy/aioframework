import asyncio

import mmh3
import math
import random


class AioBloomFilter(object):
    """
    异步布隆过滤
    适配异步redis
    """

    # 内置100个随机种子
    # SEEDS = [random.randint(100, 1000) for _ in range(100)]
    SEEDS = [377, 317, 375, 507, 542, 505, 416, 449, 209, 520, 126, 581, 480, 912, 281, 117, 100, 238, 786, 604, 141, 992, 227, 450, 735, 664, 913, 155, 984, 338, 540, 773, 180, 217, 402, 293, 926, 653, 899, 900, 333, 917, 651, 608, 310, 578, 780, 775, 457, 922, 590, 953, 801, 999, 109, 344, 247, 763, 748, 253, 750, 685, 962, 287, 498, 600, 585, 186, 906, 846, 368, 676, 352, 907, 403, 640, 655, 891, 752, 405, 462, 451, 710, 616, 806, 447, 620, 122, 923, 825, 785, 993, 266, 346, 798, 362, 974, 430, 298, 810]


    # capacity是预先估计要去重的数量
    # error_rate表示错误率
    # conn_obj表示redis的连接客户端
    # key表示在redis中的键的名字前缀
    def __init__(self, capacity=100000000, error_rate=0.00001, conn_obj=None, key='BloomFilter'):
        self.m = math.ceil(capacity * math.log2(math.e) * math.log2(1 / error_rate))  # 需要的总bit位数
        self.k = math.ceil(math.log1p(2) * self.m / capacity)  # 需要最少的hash次数
        self.mem = math.ceil(self.m / 8 / 1024 / 1024)  # 需要的多少M内存
        self.blocknum = math.ceil(self.mem / 366)  # 需要多少个366M的内存块,value的第一个字符必须是ascii码，所有最多有256个内存块
        self.seeds = self.SEEDS[0:self.k]
        self.key = key
        self.N = 2 ** 31 - 1
        self._redis = conn_obj

    async def add(self, value):
        hashs = self.get_hashs(value)
        for hash in hashs:
            await self._redis.setbit(self.key, hash, 1)

    async def is_exist(self, value):
        """

        :param value:
        :return: True: 已存在; False: 未存在
        """

        hashs = self.get_hashs(value)
        exist = True
        for hash in hashs:
            exist = exist & await self._redis.getbit(self.key, hash)
        return exist

    def get_hashs(self, value):
        hashs = list()
        for seed in self.seeds:
            hash = mmh3.hash(value, seed)
            if hash >= 0:
                hashs.append(hash)
            else:
                hashs.append(self.N - hash)
        return hashs





