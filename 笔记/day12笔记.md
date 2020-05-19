# 复习

1. 每天关虚拟机的时候, 先关`kafka`

2. 起不来怎么办?

   - 把`logs`目录删除(三个节点一起删)
   - `rmr /mykafka`

3. `0.10 kafka集成`

4. 转换

   - 无状态

     - `transform(rdd => rdd...)`

   - 有状态

     - 需要`checkPoint`

     - `updateStateByKey((seq, opt) => )`
     - 窗口操作
       - `reduceByKeyAndWindow...`
       - 直接给流加窗口`.window(, )`

5. 输出

   ```scala
   .print
   .foreachRDD(rdd => {
       rdd.foreachPartition(it => {
           // 建立连接
           // 写
           // 关闭连接
       })
   })   // 用来把数据写入到外部存储
   ```

   

# 需求2

```scala
统计各广告最近 1 小时内的点击量趋势：
	各广告最近 1 小时内各分钟的点击量, 每6秒更新一次
用到窗口:
	窗口的长度: 1h
	窗口的步长: 6s

((ads, hm), 1)  做聚合
```

