# 一. 昨日内容回顾

`spark-sql`: 处理结构化数据

入口: `SparkSession`

```
val spark = SparkSession.builder()
.master()
.appname()
.getOrCreate()
```

`DataFrame`

- 如何创建

  - 从`json`来读数据, 得到`DF`

    ```scala
    val df = spark.read.json(...)
    ```

  - 把`rdd`转成`df`

    ```scala
    1. 导入隐式转换
    import spark.implicitly._
    2. rdd.toDF(列名,...)
    ```

`DataSet`

`DF编程模式`

- `sql`(重点)

  ```scala
  1. 创建临时表
  df.createOrReplaceTempView("user")
  2. 在临时表上进行查询
  val df1 = spark.sql("select ....")
  df1.show
  ```

- `dsl`

  了解

  ```scala
  df.groupBy
  df.select
  df.filter
  ...
  map
  ...
  ```

`df和rdd`之间的转换

```scala
rdd->df
 0. 前提需要隐式转换
 1.先让rdd存储样例类
 2.rdd.toDF
df->rdd
 不需要额外的操作
 val rdd:  RDD[Row] = df.rdd
  rdd.map(...)
  是因为DF中存储就是Row
```

> 关于Row
>
> 1. 其实可以把Row看成一个集合.  也有下标 0,1,2,3
>
> 2. 存在目的是存储数据. 
>
> 3. `row.getInt(0)  getString ....`
>
>    

# 一. `DataSet`

强类型. 更加安全

> 注意: sql查询, 一般只用在弱类型的df上.(也可以用到ds上)
>
> ```scala
> ds.filter(_ > 30).show()
> ```

```
rdd和ds转换
rdd->ds(简单到复杂: 需要样例类)
	1. 在rdd中存储样例类
	2. rdd.toDS
ds->rdd
	ds.rdd   (df.rdd)
```



```scala
df -> ds
 1. 有样例类
 2. df.as[样例类]

ds -> df
	ds.toDF
```



# 三. 自定义函数

`UDF` 1进1出

`UDAF`聚合函数  多进1出

`UDTF`(`flatMap`) 1进多出

> hive: 如何自定义函数
>
> 定义类, 继承系统提供的函数, 打包, 放在hive的lib目录, 还需要注册...

`spark`中自定义函数及其简单: `scala` 是函数式编程, 所以自定义函数, 直接用时候用匿名函数非常方便.

- `udf`

  ```scala
  spark.udf.register("myToUpper", (s: String) => s.toUpperCase)
  ```

- `udaf`

  - `UserDefinedAggregateFunction`

    只用用于`df`上. 只能用在`sql`语句中.

# 四. 数据源

## 读

- 一种通用写法

  ```scala
  spark.read.format("json").load("examples/src/main/resources/people.json")
  ```

  `json`表示文件个数

  `load` 路径

  `format`默认格式`parquet`

- 一种专用写法

  ```scala
  spark.read.json("")   // foramt("json").load("")
  ```

正常情况, 如果要执行`sql`, 要先创建及临时表. 其实`spark-sql`支持直接在文件上执行`sql`

```scala
spark.sql("select * from json.`examples/src/main/resources/people.json`").show
```

## 写

- 一种通用的写

  ```scala
  df.write.format("json").save("./user")
  ```

  在写的时候, 如果路径已经存在, 则默认抛出异常!

  可以使用模式来控制:

  ```scala
  .mode("error")  // 默认值
  "append"  //追加
  "overwrite"  // 覆盖
  "ignore"  // 如果文件存在忽略这次操作. 不存在, 则写入
  ```

  

- 一种转用的写

  ```scala
  df.write.mode("append").json("./user")
  ```



# 五 `spark`整合`hive`







