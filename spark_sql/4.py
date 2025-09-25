from pyspark.sql import SparkSession


# 1.构建SparkSession
spark = (SparkSession.builder
         .appName("apple")
         .config("spark.sql.adaptive", "true")
         .config("spark.jars", r"C:\Program Files\spark-3\jars\mysql-connector-j-8.4.0.jar")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


# 2.读取日志
df = (spark.read.text("hdfs://cdh01:8020/weblog/access_2013_05_30.log")
.selectExpr(
    "split(value,' ')[0] as ip",
    "split(value,' ')[3] as time",
    "split(value,' ')[5] as url",
    "split(value,' ')[8] as status"))

# 刷新元数据并重新创建视图
spark.catalog.clearCache()  # 清除缓存
df.createOrReplaceTempView("abc")  # 重新创建视图

# 再次尝试查询

test1 = spark.sql("SELECT * FROM abc")
# test1.show(5)


# 3）业务1：统计每天PV浏览量，编写SparkSQL，展示结果；（10分）

test2 = spark.sql(
    """
    
    SELECT substring(time,13,2) as day,
            count(*) as pv
    FROM abc
    group by substring(time,13,2)
    order by day
    """
)

# test2.show()



# 4）、业务2：统计每天注册用户数，编写SparkSQL，展示结果；（10分）


test3 = spark.sql(
    """
    
    SELECT substring(time,13,2) as day,
            count(*) as reg_users
    FROM abc
    where url LIKE '%register%' 
    group by substring(time,13,2)
    order by day
    """
)

