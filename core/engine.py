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

        def process_performance(sioval: str, show_len: Optional[int] = 15):
            """
            ncalls
            调用次数

            tottime
            在指定函数中消耗的总时间（不包括调用子函数的时间）

            percall
            是 tottime 除以 ncalls 的商

            cumtime
            指定的函数及其所有子函数（从调用到退出）消耗的累积时间。这个数字对于递归函数来说是准确的。

            percall
            是 cumtime 除以原始调用（次数）的商（即：函数运行一次的平均时间）

            filename:lineno(function)
            提供相应数据的每个函数

            如果第一列中有两个数字（例如3/1），则表示函数递归。第二个值是原始调用次数，第一个是调用的总次数。请注意，当函数不递归时，这两个值是相同的，并且只打印单个数字。
            """

            if sioval == "":
                return [{}, ""]
            if show_len <= 0:
                show_len = 15

            sioval = sioval.split("\n")

            perforMap = {}
            perforLst = []
            flag = False
            for line in sioval:
                line = line.strip()
                if not line:
                    continue

                if "function" in line and "in" in line and "seconds" in line:
                    line = line.split()
                    perforMap["total_func"] = int(line[0])
                    perforMap["total_seconds"] = float(line[-2])
                elif line == "ncalls  tottime  percall  cumtime  percall filename:lineno(function)":
                    flag = True
                    continue

                if not flag:
                    continue

                line = line.split()
                perfor = {
                    "ncalls": int(line[0]),
                    "tottime": float(line[1]),
                    "percall": float(line[2]),
                    "cumtime": float(line[3]),
                    "percall1": float(line[4]),
                    "func_info": "".join(line[5:]),  # filename:lineno(function)
                }
                perforLst.append(perfor)

            perforLst = sorted(perforLst, reverse=True, key=lambda x: x['cumtime'])
            perforMap["performance"] = perforLst
            # print(perforMap)

            c = 0
            s = "    {0:^8s} {1:^8s} {2:^8s} {3:^8s}\n".format(*["总耗时", "调用次数", "平均每次耗时", "函数信息"])
            for perfor in perforLst:
                if perfor["func_info"].startswith("{") and perfor["func_info"].endswith("}"):
                    continue

                l = "{:^4d}{cumtime:^10.3f} {ncalls:^10d} {percall1:^10.3f}  {func_info}\n".format(c, **perfor)
                s += l
                if c + 1 == show_len:
                    s += "  ...\n\n"
                    break
                else:
                    c += 1

            s += f"总调用次数：{perforMap['total_func']}    总耗时：{perforMap['total_seconds']}s"

            return [perforMap, s]

        import cProfile, pstats, io

        with cProfile.Profile() as pr:

            # 启动代码
            self.logger.info(f"[Task start]>> ...")
            self._loop.run_until_complete(self.run_task())
            self.logger.info(f"[Task end]>> ...")

            sio = io.StringIO()
            ps = pstats.Stats(pr, stream=sio).sort_stats("cumtime")
            ps.print_stats()

            [_, s] = process_performance(sio.getvalue())
            self.logger.info("性能统计：\n" + s)

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
    #         for url in urls * 100:
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
