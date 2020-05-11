# 把scala的集合的数据放到RDD中如何分区

```scala
val list = List(1,2,3,4,5,6)
sc.parallelize(list)
默认分分区数: 申请到的cpu的核心数
sc.parallelize(list, 2)  // 指定分区数
区别: 5 表示是rdd的分区数
		cpu的核心数(2), 是在启动app的申请. 表示的是最大并行度


```

# 读取文件的时候如何分区

```scala
def textFile(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
    assertNotStopped()
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }

```

最小分区数:

- 要么是1要么是2, **一般都是2**
- `minPartitions = 2`



```java
// 对传入有文件切片. 每个切片对应rdd中的一个分区
val inputSplits = inputFormat.getSplits(jobConf, minPartitions)

// 列出传入目录下所有的文件
FileStatus[] files = listStatus(job);

long totalSize = 0;   // 所有文件的总大小  1400 B

// 目标尺寸:goalSize =  1400 / (2 == 0 ? 1 : 2)  =  700 B
long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
// minSize = Math.max(1, 1) = 1   // 表示每个切片的最小长度
long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);

遍历每个文件:

long blockSize = file.getBlockSize();  // 本地文件是: 32M
// 每个切片应该多大? (700, 1, 32M)   splitSize = 700
long splitSize = computeSplitSize(goalSize, minSize, blockSize);
// 待且的长度
long bytesRemaining = length;
```

总结: 

1. 文件的尺寸和总大小的一半的1.1倍(或者32M或者128M)做比较
2. 如果大于, 则, 否则不且

# 需求1:

`Top10`热门品类:

一次计算3个指标:

使用累加器, 一次计算出来3个指标!!!



## 需求2:

1. 优化方法1: 

   ​	需要排序, 所以把`Iterable`转成集合, 然后再排.

      ???

      `RDD`有没有排序功能?`sortBy  sortByKey`  他们的排序不会内存溢出.  借助磁盘

      能用, 但是不能直接用.  

     `RDD`的是全局排序

     排10次(10个job), 一次排1个品类的session

   - 优点
     - 利用了`RDD`的排序功能, 所以不会`oom`, 任务一定能跑完.
   - 缺点:
     - `job`太多, 每个品类, 需要单独起一个`job`来完成.

