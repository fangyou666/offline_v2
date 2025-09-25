from pyspark.sql import SparkSession
from pyspark.sql.functions import split, to_date, col, cast

spark = SparkSession.builder.appName("apple_stock").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = (spark.read.text("hdfs://cdh01:8020/data/stock/apple-stock.csv")
.filter("value not like 'Date%'")
.select(
    to_date(split(col("value"), ",")[0], "yyyy/M/d").alias("trade_date"),
    split(col("value"), ",")[1].cast("double").alias("open_price"),
    split(col("value"), ",")[2].cast("double").alias("high_price"),
    split(col("value"), ",")[3].cast("double").alias("low_price"),
    split(col("value"), ",")[4].cast("double").alias("close_price"),
    split(col("value"), ",")[6].cast("bigint").alias("volume")
))

df.createOrReplaceTempView("app")

# test1 = spark.sql("""
#     SELECT
#         YEAR(trade_date) AS year,
#         SUM(volume) AS total_volume
#     FROM app
#     GROUP BY YEAR(trade_date)
#     ORDER BY total_volume DESC
# """)
#
# test1.show()


