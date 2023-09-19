from asyncio import Queue, QueueFull



class AioQueue(Queue):
    """
    继承异步queue，异步队列使用collections.deque()
    def _init(self, maxsize):
        self._queue = collections.deque()

    def _get(self):
        # 移除最左测一个元素
        return self._queue.popleft()

    def _put(self, item):
        # 添加 item 到右端
        self._queue.append(item)

    现增加方法：添加 item 到左端
    """


    def _put_left(self, item):
        self._queue.appendleft(item)


    def put_left_nowait(self, item):
        """Put an item into the queue without blocking.

        If no free slot is immediately available, raise QueueFull.
        """
        if self.full():
            raise QueueFull
        self._put_left(item)
        self._unfinished_tasks += 1
        self._finished.clear()
        self._wakeup_next(self._getters)

    async def put_left(self, item):
        """Put an item into the queue.

        Put an item into the queue. If the queue is full, wait until a free
        slot is available before adding item.
        """
        while self.full():
            putter = self._get_loop().create_future()
            self._putters.append(putter)
            try:
                await putter
            except:
                putter.cancel()  # Just in case putter is not done yet.
                try:
                    # Clean self._putters from canceled putters.
                    self._putters.remove(putter)
                except ValueError:
                    # The putter could be removed from self._putters by a
                    # previous get_nowait call.
                    pass
                if not self.full() and not putter.cancelled():
                    # We were woken up by get_nowait(), but can't take
                    # the call.  Wake up the next in line.
                    self._wakeup_next(self._putters)
                raise
        return self.put_left_nowait(item)




# async def queue_test():
#     q = AioQueue(maxsize=3)
#
#     await q.put(1)
#     await q.put(2)
#     await q.put(3)
#     await q.put_left(4)
#
#     while q.qsize() > 0:
#         num = await q.get()
#         print(num)
#
# import asyncio
# asyncio.run(queue_test())





