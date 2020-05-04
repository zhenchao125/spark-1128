# day01笔记

大数据: 

存储

- `hadoop(hdfs)`, `hbase(底层hdfs)`

传输

- `flume`, `sqoop`

计算

- `hadoop(mr)`, `tez`, `spark`

## 一. 设备的准备

1. 虚拟机3台
2. `hadoop`
3. `hive`
4. `hbase`
5. `kafka`
6. `mysql`

> 注意: 在我们学习阶段, 千万不要配置`SPARK_HOME`环境变量

## 二. local模式

```scala
bin/spark-submit \                                                                 
--class org.apache.spark.examples.SparkPi \
--master 'local[*]' \
./examples/jars/spark-examples_2.11-2.1.1.jar \
100
```

## Spar-shell

1. 准备数据()

2. 启动`spark-shell`

   ```scala
   bin/spark-shell --master 'local[2]'
   ```

   注意: 可以省略`--masetr 'local[2]'`, 默认就是 `--master 'local[*]'`

3. 计算`wordCount`

   ```scala
   sc.textFile("./input").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect
   //  hello  -> (hello, 1)
   
   // atguigu-> (atguigu, 1)
   ```

## 一些核心概念



## ` 三.Standalone`模式

是集群模式, 是真正的分布式. 

搭建集群的时候,只有`spark`就可以了, 不需要其他任何的框架.

是spark自带.

### 搭建`spark`集群

1. 配置环境变量 `spark-env.sh`

   ```scala
   SPARK_MASTER_HOST=hadoop102
   SPARK_MASTER_PORT=7077   # master的端口, 提交应用的时候通过这个端口
   
   ```

2. slaves

   ```scala
   hadoop102
   hadoop103
   hadoop104
   ```

3. 分发

4. 启动集群

   ```
   sbin/start-all.sh
   ```

   > 1. 在当前设备启动一个`master`
   >
   >    要和你配置中的`master`保存一致
   >
   > 2. 在`salves`的每个`slave`起动一个`worder`

   通过ui查看worker和master

   1. `master`  8080
   2. `worder` 8081

### 向集群提交任务

1. 运行Pi

2. wordcount

   保证文件每个`node`都有

### 上午内容的总结

1. 如何在`local`模式提交应用, 和运行`spark-shell`

   ```scala
   bin/spark-submit 
   ```

2. 配置`Standalone`模式

   - 运行`pi`

     ```scala
     --master spark://hdooop102:7977
     ```

   - 启动`spark-shell`

   - `workdcount`

     > 计算的文件, 要每个节点都有一份

### 关于ui地址

master: 8080

worker: 8081

application: 4040  (用来查看正在运行的`app`的情况, 一旦这个应用结束, 则无法查看)

如何解决日志问题: 配置历史服务器

1. `spark-defaults.conf`

   默认配置

   ```scala
   spark.eventLog.enabled           true
   spark.eventLog.dir               hdfs://hadoop102:9000/spark-log-dir-1128
   ```

   > 注意: 目录要手动提前创建好

2. `spark-env.sh`

   ```shell
   export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080 -Dspark.history.retainedApplications=30 -Dspark.history.fs.logDirectory=hdfs://hadoop102:9000/spark-log-dir-1128"
   ```

   > 不要忘记启动hdfs

3. 启动历史服务器

   重启集群

   ```scala
   sbin/start-history-server.sh
   ```


### `deploy-mode`这个参数

当在集群中运行spark的时候, `driver`他的运行位置有两个选择:

1. 在客户端(`client`)

   默认就是这种情况

   ```scala
   --deploy-mode client    (默认值)
   ```

   这个时候的`driver`所有日志已经在控制台打印完毕, 所以历史服务器不会有记录

   集群在北京, 提交应用在深圳.   这个时候驱动在深圳

2. 在集群中某个节点上

   ```scala
   --deploy-mode cluster
   ```

   这个时候的driver就运行在集群中,要看`driver`的日志, 需要去worker上去找

## `Master` 需要高可用

以前配置过的高可用:

1. `nn`
2. `rm`
3. `hmaster`

-----

需要做的的事情:

1. 注释掉关于`master`的配置

   ```shell
   #SPARK_MASTER_HOST=hadoop102
   #SPARK_MASTER_PORT=7077   # master的端口, 提交应用的时候通过这个端口
   ```

2. 添加高可用配置

   ```shell
   export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=hadoop102:2181,hadoop103:2181,hadoop104:2181 -Dspark.deploy.zookeeper.dir=/spark1128"
   ```

3. 分发

4. 需要启动多个`master`

# 四. yarn模式

将来不需要额外的搭建新的集群, 只需要把要运行的任务`bin/spark-submit` 交个`yarn`集群

## 配置

1. copy standalone模式, 为了使用里面的已经配置好的日志服务. 高可用去掉

2. 配置, 让spark知道`rm`的位置. 向yarn提交, 本质就是连接`rm`

   只需要配置一下` hadoop`的配置文件的目录

   ```shell
   HADOOP_CONF_DIR=/opt/module/hadoop-2.7.2/etc/hadoop
   ```

   或者

   ```shell
   YARN_CONF_DIR=/opt/module/hadoop-2.7.2/etc/hadoop
   ```



## 运行任务

```shell
bin/spark-shell --master yarn
```

注意: spark-shell运行在yarn的实时, `deploy-mode`只能是`client`

可能碰到的问题:

报`lzo`的jar没有

1. 第一种解决方案:在`hadoop`去掉相关的配置
2. 第二种解决方案: 在spark指定下`lzo jar`的位置



## `idea`的`module`的概念

idea中的代码:

1. 如果在idea中直接运行, 则必须有`setMaster("local[*]")`
2. 如果要打包到`linux`执行, 则必须把``setMaster("local[*]")``去掉., 在使用`spark-submit`指定`master`



# 五. 当日内容总结:

1. local模式运行要会
2. standalone模式的配置要完成(优先级比较低1)
   - 日志配置
   - 高可用(了解)
3. yarn模式要配置完成(优先级高一点2 )
   - 日志
4. 在idea中能正常的运行代码(今天完成即使不睡觉也要完成)
   - jar下不来.