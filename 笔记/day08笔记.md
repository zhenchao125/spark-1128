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



