# 一. 昨日内容回顾

```scala
rdd.persist   // 持久化
// 清除持久化数据, 可以让spark自动清除. LRU
// 也可以手动清除
rdd.unpersist  // 释放内存


rdd.map(x => 如果用到自定义的类型)
自定义类型必须要支持序列化
	自定义类型的方法和属性
```

1. 行动算子

   ```scala
   foreach
   foreachPartition
   ```

2. 函数的传递

   序列化.  

   1. `java`的序列化
   2. `kryo`序列化
      - 注册需要序列化的类
      - 类也要实现`Seralizable`接口
      - `case class`

3. 依赖

   宽依赖和窄依赖

4. `job`的划分

   - `application `
   - `job` 
   - `stage` 
   - `task` 和分区数对应.
     - 将来一个节点(设备)可以运行多个`executor`(进程)
     - 一个executor可以运行多个`task`, 每个`task`是一个线程
     - 核心数表示能够同时运行的`task`数量
     - 比如: `100`task, 只申请到了`10`个核心

5. `DAG` 有向无环图

6. 持久化

7. `checkpoint`

8. 分区器

   - 只有`kv`形式才可能有分区器

   - 分区器是按照`k`来进行分区

   - 自定义分区器

     ```scala
     def numPartitions
     def getPartition(key:Any):Int
     ```

   - `RangPartitioner`

     最核心的是确定边界数组. 使用水塘抽样

# 读写文件

## 写文本文件

   ```scala
   rdd.saveAsTextFile(路径)
   每个分区一个文件
   ```

   ## 读`json`文件

   本质还是读文本文件, 然后使用`json`工具解析出来

   > `json`文件, 必须保证每行是一个完整`json`数据
   >
   > 下面这个不行:
   >
   > ```scala
   > {
   >     "name": "zs",
   >     "age": 20
   > }
   > ```
   >
   > 这个才行: 
   >
   > ```scala
   > {"name":"Michael"}
   > {"name":"Andy", "age":30}
   > {"name":"Justin", "age":19}
   > ```
   >
   > 

   ```
   import scala.util.parsing.json.JSON
   val rdd2 = rdd1.map(x => JSON.parseFull(x))
   ```

   > 这个仅仅做了解, 后期使用`spark-sql`, 就非常简单

## `seqenceFile`

   `rdd`必须是`kv`格式

```scala
val rdd1 = sc.parallelize(Array("a" ->97, "b" -> 98, "c" -> 99))
rdd1.saveAsSequenceFile("./seq")
```

读:

```scala
val rdd1 = sc.sequenceFile[String, Int]("./seq")
```

注意: 一定要要指定`k和v`的泛型

## `objectFile`

任何的`rdd`都可以保存

```scala
rdd1.saveAsObjectFile("./obj")
val rdd1 = sc.objectFile[String]("./obj")
```







