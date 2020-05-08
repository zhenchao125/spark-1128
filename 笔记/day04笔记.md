# 一 昨日内容回顾

`kv`形式`RDD`专用的的算子, `...byKey` 都会`shuffle`

1. `partitionBy(分区器)`
   - 对`RDD`进行重新分区, 使用传入的分区器
   - `(k,v)` 按照`k`来选分区
   - 分区器只有`kv`的才可能有, `单value`的一定没有
2. `reduceByKey, foldByKey, aggregateByKey, combineByKey`
   - ``reduceByKey, foldByKey`  分区内聚合和分区间聚合的逻辑一样
   - `aggregateByKey, combineByKey` 分区内聚合和分区间聚合可以不一样
   - `foldByKey, aggregateByKey, combineByKe` `zero`只在分区内聚合的时候使用
3. `sortByKey`
   - 只能用于`kv`形式的`RDD`排序.
   - `sortBy` 可以用于任何形式的`RDD`排序.
4. `RDD`的`join`
   - k相等其实就是连接条件
5. `cogroup`
   - 一种`join`

# 二 行动算子

```scala
val rdd2 = rdd1.filter(x => {
            println("filter " + x)
            x > 30
        }).map(x => {
            println("map " + x)
            x * x
        })
        
```

`.map .filter`这些算子的调用是在驱动端完成.`DAG`是在驱动端完成.

传入匿名函数的执行都是`lazy`, 将来都是在`executor`(进程)上执行, `executor`会启动任务(`task`, 线程)来执行.

当碰到行动算子的时候, 才开始安装`DAG`来运算.

同一个`stage`(阶段)内的`task`是并行运算. 

`reduceByKey`和`countByKey`

- `reduceByKey`是一个转换算子, 聚合把相同key的value聚合在一起
- `countByKey`是一个行动算子.仅仅是对key进行计数. 和value的值没有任何的关系.
- `countByKey`其实就是利用`reduceByKey`来计算



1. `foreach`

   ```
   foreach
       是遍历RDD中的每个元素
       
   将来我计算后的数据, 有的时候会存储到外部存储, 比如mysql
   可以用来与外部存储进行通信. 把数据写入到外部存储 . 比如mysql
   ```

2. `ruduce, fold, aggregate`

   - 行动算子. 所有的`RDD`都使用.

   - 使用方式和`scala`类似, 现在是分布式

   - `rdd1.reduce((x, y) => x + y)` 分区内聚合和分区间的聚合逻辑一样

   - `fold`的零值类型的, 必须和`RDD`中元素的类型保持一致.   零值, 在每个分区内聚合的时候使用一次, 分区间聚合的时候也会使用一次!!!  参数运算的次数是: 分区数 + 1

   - `reduce与fold分区内和分区间的聚合逻辑一样`

   - ```
     aggregate 分区内和分区间的计算不一样.  
     ```

   - 这三个用的不多.了解就行了. 一般都是应转换型的聚合算子.

