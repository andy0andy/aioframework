import asyncio
from typing import *

"""
任务模板
"""



class FunctionCall(object):
    """
    函数调用
    """

    callback: Callable
    args: Tuple
    kwargs: Dict


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

    def yield_step(self, callback: Callable, *args, **kwargs):
        """
        异步链 下一步
        用以生成 一个对象 传给异步队列调度
        :param callback:
        :param args:
        :param kwargs:
        :return:
        """

        fc = FunctionCall()
        fc.callback = callback
        fc.args = args
        fc.kwargs = kwargs

        return fc

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
