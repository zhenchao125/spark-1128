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

  ## 序列化的问题

当我传递给算子的匿名函数是个闭包. 要保证匿名函数中用到的一些属性或者方法要支持序列化

- 函数
- 属性

1. 让类序列化
2. 使用匿名函数
   - 使用局部遍历

新的序列化机制:

`kryo`, 是一个第三方的序列化机制. 比`java`的序列化机制要快,和轻量级.

`spark2.0`才开始支持. 在内部,值类型和值类型的数组已经默认采用这种机制

自定义类型, 需要做一些配置:

```scala
// 更好序列器
.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
// 注册需要序列化的类
.registerKryoClasses(Array(classOf[Searcher2]))
```



> 注意: 即使换成了`kryo`序列化, 自定义的类型也需要实现`Serializable`

# job的划分

```
application
	应用. 创建一个SparkContext可以认为创建了一个Application.
job
	在一个application中, 每执行一次行动算子, 就会创建一个job
stage
	阶段. 默认会有一个stage, 再每碰到一个shuffle算子, 会产生一个新的stage
	一个job中,可以包含多个stage
task
	任务. 表示咱们阶段执行的时候的并行度.
	假设一个RDD 有100个分区. 处理的时候,每个分区的数据, 分配一个task来计算
	一个阶段会有多个task. 
	分区, 是站数据的存储角度
	task, 是站的计算的角度
	分数数和task是相等.
	
	
总结:
	application
		多个job
			多个stage
				多个task
				
dag: 有向无环图
```

# 持久化和checkpoint的比较

1. 持久化

   ```scala
   1. 使用方式
       rdd.persist(存储级别)
       rdd.cache
   2. 不会重新起job来专门的持久化. 而是使用上一个job的结果来进行持久化
   3. 血缘还在. rdd2 的血缘关系还在. 一旦持久化的出现问题, 可以通过血缘关系重建rdd
   ```

2. `checkepoint`

   ```scala
   1. 使用方式
      sc.setCheckpointDir("./ck1")
      rdd.checkpoint()
   2. checkpoint的时候, 会重新启动一个新的job来专门的做checkpoint
   3. checkpoint会切断 rdd 的血缘关系.
   ```

   > 不管是持久化还是`checkepoint`, 都是针对在同一个` app`中使用
   >
   > `rdd`被重复使用才需要做缓存或checkpoint

   > 注意: 在实际使用的时候, 一般会把缓存和checkpoint配合起来使用.

# 分区器的一些概念

用来在`rdd` `shuffle`, 之后那些`kv`值应该进入到哪个分区来服务的.

默认的分区器一般都是`HashPartitioner`

```scala
(10, v) 20.. 30 10 50 100
new HashPartioner(2)
严重的数据倾斜
```

1. 自定义分区
2. `spark`提供了一个分区器, 可以最大程度的避免数据倾斜.`RangePartitioner`



`RangePartitioner`

范围分区器. 为什么需要范围分区器? 原因是`HashPartitioner`会在某些情况下造成数据倾斜.

```scala
100w 放到4个分区
理想情况是: 每个分区25w条

根据这些数据的key, 来生成一个数组, 这个数组可以把数据做到4等分. 这个是数组存储其实是边界条件
边界数组

边界数组: [100, 200, 300]
如何确定边界数组? 
	抽样. 
	水塘抽样

```















