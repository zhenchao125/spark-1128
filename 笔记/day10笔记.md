# 一. 问题

- `lzo`的问题

  - `core-site`中的`lzo`的配置去掉

  - 获取去掉`core-site`

- `MetaException(message:For direct MetaStore DB connections, we don't support retries at the client level.);
  	at org.apache.spark.sql.hive.HiveExternalCatalog.withClient(HiveExternalCatalog.scala:106)`

  - 更好`mysql`驱动
  - `5.1.27` 版本

# 二 spark-streaming

处理流失数据.  实时处理

## `Dstream`的创建

1. 通过`socket`来创建
   - 一般用于测试或学习阶段
   - 生产环境不会使用
2. 通过`RDD`队列来创建
   - 一般用于做测试, 压力测试.
   - 测试集群的计算能力

## Kafka数据源

三个语义:

1. 至多一次(高效, 数据丢失)
2. 正好一次(最理想. 额外的很多工作, 效率最低)
3. 至少一次(保证数据不丢失, 数据重复)



`spark-streaming` 直连模式, 只是做到了消费的时候的严格一次.  如何向输出的也要严格一次,

需要开发者自己保证: 1. 输出系统是幂等  或2. 输出系统支持事务

