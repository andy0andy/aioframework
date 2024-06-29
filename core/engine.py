import asyncio
from typing import *

try:
    from task import AioTask, FunctionCall
    from config import AioConfig
    from utils.queues import AioQueue
except:
    from core.task import AioTask, FunctionCall
    from core.config import AioConfig
    from core.utils.queues import AioQueue

from settings import *




class AioEngine(AioConfig, AioTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._q = AioQueue(maxsize=QUEUE_SIZE)
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._sem = asyncio.Semaphore(SEMAPHORE)
        self.lock = asyncio.Lock()

        # 参数 初始化


    async def add_tasks(self):
        """
        异步添加任务
        :param tasks:
        :return:
        """

        try:
            async for task in aiter(self.publish_tasks()):
                if task is None:
                    continue

                self.logger.debug(f"[Add queue]>> {str(task)[:100]+'...' if len(str(task))>100 else task}")
                func_call = self.yield_step(self.process, task)
                await self._q.put(func_call)
        except Exception as e:
            self.logger.exception(f"[Error]>> add_tasks - {e}")
        finally:
            for _ in range(TASK_SIZE):
                await self._q.put(None)

    async def do_tasks(self):
        """
        异步处理任务
        :return:
        """

        while True:
            func_call = await self._q.get()
            try:
                if func_call is None:
                    break

                # 开始处理任务
                async with self._sem:

                    if isinstance(func_call, FunctionCall):

                        coroutine_obj = func_call.callback(*func_call.args, **func_call.kwargs)
                        if hasattr(coroutine_obj, "__aiter__") and hasattr(coroutine_obj, "__anext__"):  # 判断是否是异步迭代对象
                            async for next_func_call in aiter(coroutine_obj):

                                log_str = f"[Add queue]>> Next step:\n\tCallback name: {next_func_call.callback.__name__}\n\tArgs: {str(func_call.args)[:100]}\n\tKwargs: {str(func_call.kwargs)[:100]}"
                                self.logger.debug(log_str)

                                await self._q.put_left(next_func_call)
                        else:
                            log_str = f"[Out queue]>> \n\tCallback name: {func_call.callback.__name__}\n\tArgs: {str(func_call.args)[:100]}\n\tKwargs: {str(func_call.kwargs)[:100]}"
                            self.logger.debug(log_str)

                            await coroutine_obj

            except Exception as e:
                log_str = f"[Error]>> do_tasks - {e}\n\tCallback name: {func_call.callback.__name__}\n\tArgs: {str(func_call.args)[:100]}\n\tKwargs: {str(func_call.kwargs)[:100]}"
                self.logger.exception(log_str)
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
                    "ncalls": line[0],
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

                l = "{:^4d}{cumtime:^10.3f} {ncalls:^10s} {percall1:^10.3f}  {func_info}\n".format(c, **perfor)
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

            # 异步程序启动
            self.logger.info(f"[Task start]>> ...")

            self._loop.run_until_complete(self.start_run())
            self._loop.run_until_complete(self.run_task())
            self._loop.run_until_complete(self.end_run())

            self.logger.info(f"[Task end]>> ...")

            sio = io.StringIO()
            ps = pstats.Stats(pr, stream=sio).sort_stats("cumtime")
            ps.print_stats()

            [_, s] = process_performance(sio.getvalue())
            self.logger.info("性能统计：\n" + s)



