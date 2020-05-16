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



