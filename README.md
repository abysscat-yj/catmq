# CatMQ 分布式消息中间件
参考 kafka、pulsar 实现。

## 当前进展
* 初始化项目骨架
* 实现基于内存阻塞队列的生产消费逻辑，包含拉模式和推模式
* 实现 MQ Server API 接口
* 增加 MQ Client 方法调用 MQ Server
* 实现 MQ 消息文件持久化