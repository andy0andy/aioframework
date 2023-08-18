import asyncio
import aiohttp
from tornado import ioloop, queues
import random

urls = [
    "https://cn.element14.com/texas-instruments/ads7924irter/adc-octal-sar-12bit-100ksps-wqfn/dp/2782707RL?st=ads7924irter",
    "https://cn.element14.com/phoenix-contact/3006043/terminal-block-din-rail-2pos/dp/3042960",
    "https://cn.element14.com/power-integrations/lnk3604p/off-line-switcher-ic-flyback-dip/dp/2951378",
    "https://cn.element14.com/stmicroelectronics/stth1602ct/diode-ultrafast-2x8a/dp/9935878",
    "https://cn.element14.com/onsemi/es2d/diode-fast-2a-200v-smd-do-214/dp/1467491",
]


class AsyncEle14(object):

    def __init__(self):
        self.io_loop = ioloop.IOLoop().current()
        self.q = queues.Queue()
        self.pool_size = 10

    async def add_tasks(self, tasks: list):
        for task in tasks * 10:
            await self.q.put(task)
            # await asyncio.sleep(1)

        for _ in range(self.pool_size):
            await self.q.put(None)

    async def do_tasks(self):
        while True:
            item = await self.q.get()
            try:
                if item is None:
                    break
                # 在这里处理队列中的任务
                print(f"{self.q.qsize()=}  {item=}")
                await asyncio.sleep(random.random())
            finally:
                self.q.task_done()


    async def run_tasks(self):

        cs = [self.do_tasks() for _ in range(self.pool_size)]
        cs.append(self.add_tasks(urls))

        await asyncio.gather(*cs)

        await self.q.join()

    def start(self):
        print(f"start...")
        self.io_loop.run_sync(self.run_tasks)
        print(f"end...")

if __name__ == '__main__':
    AsyncEle14().start()
