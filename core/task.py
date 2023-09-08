import asyncio
from typing import *

"""
任务模板
"""


class AioTask(object):

    async def publish_tasks(self):
        """
        发布任务
        yield返回任务，再调用异步迭代器
        :return:
        """

    async def process(self, task_future: Any):
        """
        异步处理任务
        :param task_future:
        :return:
        """

    async def next_process(self, func: Callable, *args: Any):
        """
        异步链 下一步
        :param func:
        :param args:
        :return:
        """

        await asyncio.ensure_future(func(*args))

    async def start_run(self):
        """
        开始执行任务前一步
        :return:
        """


    async def end_run(self):
        """
        开始执行任务前一步
        :return:
        """
