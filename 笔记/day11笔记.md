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