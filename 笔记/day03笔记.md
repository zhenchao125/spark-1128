# 一. 昨日内容复习

学习`RDD`进行编程

1. 什么是`RDD`
   - 5个主要属性
     - 分区列表(分布式的并行运算)
     - 计算切片的函数
     - `RDD`的依赖列表
     - 可选: 只针对`kv`形式`RDD`的分区器.
     - 可选: 计算每个切片的偏好位置列表
   - 特点
     - 弹性
     - 分区
     - 只读
     - 依赖(血缘)
       - 窄依赖
       - 宽依赖
         - `coalesce`
           - 增加分区需要`shuffle`
         - `repartition`
         - `groupBy`
         - `sortBy`
     - 缓存
       - 保留血缘关系
     - `checkpoint`
       - 切断血缘关系
2. `RDD`编程
   - 转换算子
     - 只要这个算子的返回值是一个`RDD`,那么就一定是转换算子
     - 都是`lazy`的, 只要碰到一个`action`, 那么从最初的位置开始执行真正的转换
   - 行动算子
     - 返回值不是`RDD`就一定是行动算子
     - 用来计算的触发动作.
     - `job`, 如果碰到一个`action`就会创建一个`job`, 都会从转换的最初开始执行传给转换算子的那些匿名函数.
   - 创建`RDD`
     - 通过`scala`的集合
       - `sc.paralize(集合, 分区数)`
       - `sc.makeRDD(集合, 分区数)`
     - 通过外部数据源(`文件, jdbc, hive, hbase`)
       - `sc.textFile(...)`
     - 通过其他的`RDD`转换得到
       - `map`
       - ` mapPartitions`(it => 返回一个集合)
       - `mapPartitionsWithIndex`
       - `flatMap`
       - `glom`
         - 返回一个`RDD[Array[Int]]`, 存储的是数组, 每个数组的数据是每个分区的数据
       - `filter`
         - 配合`coalesce`使用
       - `groupBy`
         - 会产生宽依赖,(`shuffle`)
       - `coalesce`和`repartition`
       - `sample`
         - 放回
         - 不放回
       - `distinct`
         - 去重的是自定义类型, 需要实现`hashCode`和`equals`, 去判断两个元素是否相等.
         - `hashCode`和`equals`要兼容:
           - 如果两个对象的`hashCode`返回值相等, 则`equals`应该返回true
           - `def hashCode = name.hashCode`
       - `sortBy`
         - 按照指标排序
         - 需要传入`Ordering`
         - `ClassTag`
       - `pipe`
         - 可以让`linux`命令或者脚本去处理`RDD`中的数据
         - 脚本或者命令每个分区执行一次
         - `read ele` 读一个`RDD`中的元素
       - `交并差`
       - `拉链`:`zip, zipPartitions, zipWithIndex`

# 二. `kv`形式的`RDD`

如果`RDD`中存储的是二维元组`(key, value)`, 就是`KV`形式的`RDD`

提供了很多算子`..ByKey`

1. `partitionBy`

   ```scala
   abstract class Partitioner extends Serializable {
     //返回分完区之后的新的RDD的分区数
     def numPartitions: Int
     // 每个键值对如何分区
     // 是由key, 和value没有任何关系
       // 返回的是分区索引
     def getPartition(key: Any): Int
   }
   
   ```

   > 注意: 分区的时候, 是根据`key`来选择分区, 和`value`没有任何关系
   >
   > 在分区的时候, 如何根据`value`进行分区?
   >
   > 交换`kv`

   - 只有`kv`形式的`RDD`才能使用分区器进行分区.
   - 使用分区器进行分区的时候, 一般会进行`shuffle`
   - `RDD1[(k, v)] ` 分区器是 `P1`, 对`RDD1`进行重新分区, 使用的分区器和`p1`相等, 那么这个时候, 不会真正的分区 .

2. 聚合算子

   ```scala
   所有聚合类的算子都有预聚合
   
   reduceByKey
       聚合算子:
           只能用在kv形式的聚合.
           按照key进行聚合, 对相同的key的value进行聚合
           
          
   
   foldByKey:
   	多一个zero
       1. zero的类型必须是v的类型一致
       2. zero只在分区内聚合(预聚合, map端)的时候参与运算. 
   		分区间聚合(最终聚合, reduce端)不参与
       3. 对一个key, zero最多参数参与n次 (n是分区数)
   	
   	
   ```

   

