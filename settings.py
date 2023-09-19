import sys, os

# 多链式任务数
TASK_SIZE = os.cpu_count() * 10

# 并发量
SEMAPHORE = 100

# 队列大小
QUEUE_SIZE = 5000

# 日志
LOG_LEVEL = "INFO"
LOG_PATH = "../logs/runtime{time:YYYY-MM-DD}.log"


# redis
REDIS_CONN = {
    "host": "1",
    "port": 1,
    "password": "1",
    "db": 1
}


# mongo
MONGO_CONN = {
    "host": "1",
    "port": 1,
    "username": "1",
    "password": "1",
}
