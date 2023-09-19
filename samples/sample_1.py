from typing import *
import asyncio


from core.engine import AioEngine


# 例子

urls = [
    "https://cn.element14.com/texas-instruments/ads7924irter/adc-octal-sar-12bit-100ksps-wqfn/dp/2782707RL?st=ads7924irter",
    "https://cn.element14.com/phoenix-contact/3006043/terminal-block-din-rail-2pos/dp/3042960",
    "https://cn.element14.com/power-integrations/lnk3604p/off-line-switcher-ic-flyback-dip/dp/2951378",
    "https://cn.element14.com/stmicroelectronics/stth1602ct/diode-ultrafast-2x8a/dp/9935878",
    "https://cn.element14.com/onsemi/es2d/diode-fast-2a-200v-smd-do-214/dp/1467491",
]

class Test(AioEngine):

    async def publish_tasks(self):
        """
        任务入口
        最初提供任务给队列的函数
        只允许是 一个异步迭代器，yield 返回
        :return:
        """

        for url in urls * 1:

            # redis实现布隆过滤器 去重，使用前需现配置redis
            is_ex = await self.filter.is_exist(url)  # 判断是否存在
            if not is_ex:
                await self.filter.add(url)  # 添加进布隆过滤器

                yield url
                await asyncio.sleep(0.1)

            yield url
            await asyncio.sleep(0.1)

    async def process(self, task_future: Any):
        """
        队列分配处理任务的第一个函数

        可以是异步对象，也可以是异步迭代器
        :param task_future:
        :return:
        """

        self.logger.success(task_future)
        await asyncio.sleep(0.2)

        if task_future == 'https://cn.element14.com/phoenix-contact/3006043/terminal-block-din-rail-2pos/dp/3042960':
            for i in range(10):
                await asyncio.sleep(0.2)

                # 异步迭代给下一步执行，在内部生成一个 FunctionCall对象 入队调度
                yield self.yield_step(self.process_next, i)

    async def process_next(self, num):

        self.logger.info(f"process_next: {num=}")


t = Test(is_dup=False)
t.run()
