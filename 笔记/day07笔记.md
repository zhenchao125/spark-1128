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
  // 临时表
  df.createTempView("user")
  df.createOrReplaceTempView("user")
  df.createGlobalTempView("person")  // 不常用
  
  spark.sql("select * from global_temp.person").show
  spark.newSession..sql("select * from global_temp.person").show
   ```

```
   
  
  
  - dsl风格
    用的不多, 但是我们会讲一些.




```