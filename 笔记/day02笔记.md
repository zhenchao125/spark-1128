# 一. 昨日内容回顾

搭建环境!!!

1. `local`模式搭建

   - 解压即用

   - 提交应用

     ```shell
     bin/spark-submit \
     --master local[2]/local[*] \
     --class 主类 \
     ./your.jar \
     给主类的main方法的参数
     ```

   - 启动`spark-shell`

     ```shell
     bin/spark-shell --master local[*]
     ```

2. `standalone`模式

   1. 集群配置

      - 两个角色
        - `master`(yarn中的RM)
        - `worder`(yarn中的NM)

   2. 使用集群

      ```shell
      --master spark://maseter:7077
      --deploy-mode client/cluster
      ```

      - client 表示driver在客户端(提交应用的位置)
      - cluster表示driver在集群的某个worker上

3. `yarn`模式

   让spark扎到yarn的相关配置

   提交: 

   ```shell
   --master yarn
   --deploy-mode client/cluster
   ```

4. idea写spark代码

   ```scala
   // 1. 创建SparkContext sc
   // 2. 通过sc读取数据(从数据源)得到第一个RDD
   // 3. 对RDD做一系列的转换(都是lazy)
   // 4. 执行一个行动算子(collect: 把执行后的数据从executor收集到driver). 才开始执行前面的转换
   // 5. 关闭sc
   ```

   - 如果是在idea中直接执行, 必须有`setMaster`
   - 如果是打包到`linux`执行, 则去掉``setMaster``, master在提交的时候通过参数指定.

# 二. 对`RDD`编程

在driver中对`RDD`编程. 

主要就是对`RDD`做转换和行动!

## 1. 创建`RDD`

1. 通过一个集合(`scala`)来得到一个`RDD`,一般用于测试和学习.

   ```scala
   val list1 = List(30, 50, 70, 60, 10, 20)
   val rdd: RDD[Int] = sc.parallelize(list1)
   //
   val rdd: RDD[Int] = sc.makeRDD(list1)
   ```

   

2. 通过外部数据读取数据(`文件, hive, jdbc,...`), 然后得到`RDD`, 生产环境都是这种.

   - 后面有一章专门研究

3. 通过另外一个`RDD`转换得到一个新的`RDD`

   - 转换研究转换算子

## 2. `map和mapPartitions`

- 都是在做`map`操作

- `map`会每个元素执行一次`map`中的匿名函数

- `mapPartitions`每个分区执行一次

  - 效率会高一些

  > 注意: `mapPartitions` 有内存溢出的风险.
  >
  > 如果你把迭代器转成容器式集合(List, Array)的时候, 如果这个分区的数据特别大, 则会内存溢出.
  >
  > 如果没有内存溢出, 则效率要比map高.

`scala`的集合如何分区?

1. 分区数如何定

   - 默认分区数: 总的核心数
   - 指定分区数

2. 如何切片

   ![](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1588648830.png)

3. `glom`

   把每个分区的数据放入到一个数组中.如果有n个分区, 得到的新的`RDD`中就有n个数组

4. `groupBy`

   需要`shuffle`, 因为这个算子会产生宽依赖.

   `shuffle`需要借助于磁盘, 所以效率比较低.  以后慎用.

   在分组的时候, 分完组之后, `rdd`是`kv`形式的, 所以, 需要重新分区.

   默认的分区器是哈希分区器!!!

5. `sample`

   数据的抽样

   参数1: 表示是否放回. 如果是true表示放回, 元素可以被重复抽到. 所以, 后面的抽样比例 [0, ∞). 如果是否`false`, 表示不放回, 元素不会被重复抽到. 所以抽样比例: `[0, 1]`

   参数2: 抽样比例

   参数3: 随机种子. 一般使用系统的时间. 如果每次种子都一样, 则抽到的值也是一样的!!!

   作用: 对比数据做评估.

6. `coalesce`

   ```
   用来改变RDD分区数.
   coalesce 只能减少分区,不能增加分区. 为啥? 因为coalesce默认是不shuffle
   如果启动shuffle, 也可以增加分区.
   
   以后, 如果减少分区, 尽量不要shuffle, 只有增加的分区的时候才shuffle
   
   实际应用:
       如果减少分区就使用 coalesce
       如果是增加分区就是用 repartition
   ```

7. `sortBy`

   ```
   sortBy
       整个RDD进行的全局排序
       参数1: 排序指标
       参数2:是否升序(默认true)
       参数3:排序后新的RDD的分区数.默认和排序前的rdd的分区数一致
   ```

8. `pipe`

   给我们一个机会, 让`linux`命令或者脚本去处理`RDD`中数据

   脚本执行情况:

   ```
   rdd1.pipe("p1.sh")
   每个分区执行一次这个脚本!!!
   ```