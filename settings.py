import sys

# 多链式任务数
TASK_SIZE = 10

# 并发量
SEMAPHORE = 10

# 队列大小
QUEUE_SIZE = 5000

# 日志
LOG_LEVEL = "INFO"
LOG_PATH = "../logs/runtime{time:YYYY-MM-DD}.log"


# redis
REDIS_CONN = {
    "host": "",
    "port": 1,
    "pass": "",
    "db": 1
}

# mongo
MONGO_CONN = {
    "host": "",
    "port": 1,
    "username": "",
    "password": "",
}


