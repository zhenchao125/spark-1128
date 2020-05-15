# 一. 昨日内容回顾

1. 自定义函数

   - 1进1出 `spark.udf.register((x, y, z) => )`

   - 多进1出:聚合函数(弱类型)

     `实现哪些抽象方法`

     `只能用在sql语句中`

2. 数据源

   ```scala
   sc.read.format("数据格式").load("路径")
   sc.read.json("路径")
   
   df.write.format("json").save("路径")
   df.write.json("路径")
   df.write.mode("error/ovewrite/append/ignore").json("")
   ```

3. `jdbc`数据源

   ```scala
   sc.read
   .option("", "")
   ...
   ```

4. 和`hive`的整合

   `hive on spark` 

   `spark on hive(spark-sql)`

   - 内置`hive`

   - 外置`hive`

     - 配置

       ```scala
       hive-site.xml  copy到 conf
       mysql 驱动      copy到 jars
       core-site.xml hdfs-site.xml  copy到conf
       
       lzo class not found  报错
       1. spark-defaults.conf 去配置一个spark.jars=...
       2. 仍然有问题: 直接把lzo的jar copy到 jars目录下
       ```

# 优化数仓脚本

```
hive=/opt/module/spark-local/bin/spark-sql

hive="/opt/module/spark-yarn/bin/spark-sql --master yarn --deply-mode client"
```

注意:

- 在yarn模式, `spark-sql, spark-shell`只支持`--deploy-moe client` 不支持``--deploy-moe cluster`