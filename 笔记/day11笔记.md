# 一. 昨日内容回顾

1. 实时计算引擎
2. 是准实时. 微批次.
3. 每批次内所有的数据, 都会封装到一个`RDD`中.
4. 数据抽象`Dstream`
5. 如何创建`Dstream`
   - 从`socket`获取
   - 从`RDD队列`
   - 自定义数据源(自定义接收器)
   - 从`kafka`创建
     - `接收器模式` 了解
     - 直连模式`direct`
       - 没有实现严格一次
       - 启用了`checkpoint`实现了严格一次



## 0.10APi

位置策略:

1. `PreferConsistent` 大多数用这个
2. `PreferBrokers` `executor`和`broker`在同一个设备
3. `PreferFixed`  当数据发生严重倾斜的时候

# 转换操作

## 无状态的转换

这个转换仅仅针对当前批次, 批次与批次之间有关系.

针对聚合算子:  用的和`RDD`一样的算子都是无状态的算子.

### ` transform`算子

流是由`RDD`组成

` transform` 可以得到每个批次内的那个`RDD`

有啥用?

流的算子不够丰富, 没有`RDD`多. 可以通过这个方法, 得到`RDD`,然后操作`RDD` 

## 有状态的转换

`updateStateByKey(*func*)`

作用:

1. 用来替换无状态的聚合函数.

### 窗口操作

每隔3秒, 计算一下最近9秒的`wordCount` ?



