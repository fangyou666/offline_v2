print(">>> 1.py 开始执行")

import logging
logging.basicConfig(level=logging.ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)

from pyspark.sql import SparkSession

# ---------- 1. 构建 SparkSession ----------
spark = (SparkSession.builder
         .appName("weblog")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.jars", r"C:\Program Files\spark-3\jars\mysql-connector-j-8.4.0.jar")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


# ---------- 2. 读取日志 ----------
df = (spark.read.text("hdfs://cdh01:8020/weblog/access_2013_05_30.log")
.selectExpr(
    "split(value,' ')[0] as ip",
    "split(value,' ')[3] as time",
    "split(value,' ')[5] as url",
    "split(value,' ')[8] as status"))
df.createOrReplaceTempView("logs")



# ---------- 3. 业务 SQL ----------
all_in_one = spark.sql("""
WITH day_base AS (
    SELECT substring(time,2,11) day FROM logs GROUP BY substring(time,2,11)
), pv AS (
    SELECT substring(time,2,11) day, COUNT(*) pv FROM logs GROUP BY day
), reg AS (
    SELECT substring(time,2,11) day, COUNT(*) reg_users
    FROM logs WHERE url LIKE '%register%' GROUP BY day
), ip AS (
    SELECT substring(time,2,11) day, COUNT(DISTINCT ip) uniq_ips FROM logs GROUP BY day
), bounce AS (
    WITH tmp AS (
        SELECT ip, substring(time,2,11) day FROM logs GROUP BY ip, day HAVING COUNT(*)=1
    ), total AS (
        SELECT substring(time,2,11) day, COUNT(DISTINCT ip) total_users FROM logs GROUP BY day
    )
    SELECT t.day, COUNT(t.ip) bounce_users,
           ROUND(100*COUNT(t.ip)/total.total_users,2) bounce_rate
    FROM tmp t JOIN total ON t.day=total.day GROUP BY t.day, total.total_users
)
SELECT b.day,
       COALESCE(p.pv,0) pv,
       COALESCE(r.reg_users,0) reg_users,
       COALESCE(i.uniq_ips,0) uniq_ips,
       COALESCE(bo.bounce_users,0) bounce_users,
       COALESCE(bo.bounce_rate,0) bounce_rate
FROM day_base b
LEFT JOIN pv p ON b.day=p.day
LEFT JOIN reg r ON b.day=r.day
LEFT JOIN ip i ON b.day=i.day
LEFT JOIN bounce bo ON b.day=bo.day
ORDER BY b.day""")




# ---------- 4. 写入 MySQL ----------
mysql_url = (
    "jdbc:mysql://192.168.200.32:3306/weblog"
    "?useUnicode=true&characterEncoding=utf8"
    "&useSSL=false"
    "&allowPublicKeyRetrieval=true"
    "&defaultAuthenticationPlugin=mysql_native_password"
)

mysql_prop = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

try:
    print(">>> 正在写入 MySQL...")
    all_in_one.write.mode("overwrite").jdbc(mysql_url, "daily_access_stat", properties=mysql_prop)
    print(">>> 写入完成，正在验证...")
    check = spark.read.jdbc(mysql_url, "daily_access_stat", properties=mysql_prop)
    count = check.count()
    print(f">>> 共写入 {count} 条记录")
    check.orderBy("day").show(truncate=False)
except Exception as e:
    print(">>> 写入 MySQL 失败：", e)

# ---------- 5. 关闭 ----------
spark.stop()