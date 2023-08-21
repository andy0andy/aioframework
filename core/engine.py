import asyncio
from typing import *

from task import AioTask
from config import AioConfig
from settings import *


class AioEngine(AioConfig, AioTask):

    def __init__(self, *args):
        super().__init__(*args)

        self._q = asyncio.Queue(QUEUE_SIZE)
        self._loop = asyncio.new_event_loop()
        self._sem = asyncio.Semaphore(SEMAPHORE)

    async def add_tasks(self):
        """
        异步添加任务
        :param tasks:
        :return:
        """

        async for task in aiter(self.publish_tasks()):
            if task is None:
                continue

            self.logger.debug(f"[Add queue]>> {task}")
            await self._q.put(task)

        for _ in range(TASK_SIZE):
            await self._q.put(None)

    async def do_tasks(self):
        """
        异步处理任务
        :return:
        """

        while True:
            task = await self._q.get()
            try:
                if task is None:
                    break

                # 开始处理任务
                async with self._sem:
                    self.logger.debug(f"[Out queue]>> {task}")
                    await self.process(task)

            finally:
                self._q.task_done()

    async def run_task(self):
        """
        执行任务
        :return:
        """

        task_list = [asyncio.ensure_future(self.do_tasks()) for _ in range(TASK_SIZE)]

        task_list.append(asyncio.ensure_future(self.add_tasks()))

        await asyncio.wait(task_list)

        await self._q.join()

    def run(self):
        """
        启动异步任务
        :return:
        """

        self.logger.info(f"[Task start]>> ...")
        self._loop.run_until_complete(self.run_task())
        self.logger.info(f"[Task end]>> ...")


if __name__ == '__main__':
    # 例子

    # urls = [
    #     "https://cn.element14.com/texas-instruments/ads7924irter/adc-octal-sar-12bit-100ksps-wqfn/dp/2782707RL?st=ads7924irter",
    #     "https://cn.element14.com/phoenix-contact/3006043/terminal-block-din-rail-2pos/dp/3042960",
    #     "https://cn.element14.com/power-integrations/lnk3604p/off-line-switcher-ic-flyback-dip/dp/2951378",
    #     "https://cn.element14.com/stmicroelectronics/stth1602ct/diode-ultrafast-2x8a/dp/9935878",
    #     "https://cn.element14.com/onsemi/es2d/diode-fast-2a-200v-smd-do-214/dp/1467491",
    # ]
    #
    # class T(AioEngine):
    #
    #     async def publish_tasks(self):
    #         for url in urls * 10:
    #
    #             yield url
    #             # await asyncio.sleep(0.5)
    #
    #     async def process(self, task_future: Any):
    #         self.logger.success(task_future)
    #
    # t = T()
    # t.run()

    # ===============
    ...
