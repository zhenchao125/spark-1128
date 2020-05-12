# 一. spark-sql

`MapReduce` -> `hive`  为了解决`mr`程序编程困难.
`spark-core` -> `spark-sql` 为了解决`rdd`编程困难

## 1.1 
`spark-core   rdd`

`spark-sql`: `DataFrame, DataSet`

## 1.2 使用`DataFrame`编程

1. 创建`DF`
 - 使用数据源(json, jdbc, csv,..)(数据源)
 - 从rdd转换过来(专门讲如何转换)
 - 通过专门查询hive(专门讲)
2. 编程风格
  - sql风格(重点)
   写sql语句
   ```scala
  // 临时表(内存级别)
  df.createTempView("user")
  df.createOrReplaceTempView("user")
  df.createGlobalTempView("person")  // 不常用
  
  spark.sql("select * from global_temp.person").show
  spark.newSession..sql("select * from global_temp.person").show
   ```

- dsl风格(了解)

  为了给那些不会sql的人准备.

  ```scala
  df.filter("salary > 3000").select("name", "salary").show
  
  // 等价于 select salary, count(*) from user group by salary
  df.groupBy("salary").count.show 
  
  // select name, sum(salary) from user group by name
  df.groupBy("name").sum("salary").show
  ```

## rdd和df的转换

很有必要.   rdd转成df, 也可以把df转成rdd

1. 在rdd中封装存储样例类的对象, 直接调用`rdd.toDF`
2. 如果不是样例类, 则需要手动指定列名`rdd.toDF(c1, c2, ...)`

注意: 要先导入隐式转换. 而且需要知道, 隐式是存在于`SparkSession`对象中. 

```scala
val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("RDD2DF")
            .getOrCreate()
import spark.implicits._
```

总结: 从简单->复杂类型, 需要额外提供样例类!!! 而且要隐式转换