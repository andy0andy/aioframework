# aioframework
异步处理任务框架，初级阶段

### 架构

1. core：核心代码文件夹
   
     - engine.py：任务调度代码，基于asyncio.Queue。使用生产者-消费者模式进行调度
     - config.py：设置代码。例如创建数据库链接对象等
     - task.py：使用代码模板。
     - utils：功能文件夹。实现一些自定义功能

2. logs：日志存储文件夹

3. settings.py：配置信息


### 功能

- 实现基于异步队列的任务调度，代码全为异步
- 实现通过yield完成异步链式调用各功能函数
- 通过redis的setbit实现布隆过滤器去重




