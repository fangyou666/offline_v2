from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("abcd") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = (spark.read.text("hdfs://cdh01:8020/data/stock/apple-stock.csv")
.selectExpr("split(value,',')[0] as data",
            "split(value,',')[1] as Open",
            "split(value,',')[2] as High",
            "split(value,',')[3] as Low",
            "split(value,',')[4] as Close",
            "split(value,',')[5] as Adj_Close",
            "split(value,',')[6] as Volume"))

df.createOrReplaceTempView("aaa")

test1 = spark.sql("select * from aaa")
# test1.show()


# | 字段名           | 含义说明                                    |
# | ------------- | --------------------------------------- |
# | **Date**      | 交易日期（年月日）                               |
# | **Open**      | 当天**开盘价**（第一笔成交价）                       |
# | **High**      | 当天**最高价**                               |
# | **Low**       | 当天**最低价**                               |
# | **Close**     | 当天**收盘价**（最后一笔成交价）                      |
# | **Adj Close** | **复权收盘价**（考虑了拆股、分红等调整后的真实价格，做回测/画图必须用它） |
# | **Volume**    | 当天**成交量**（多少股换手）                        |


# 3）业务1：编写SQL，统计每年苹果股票交易量，并按照交易量降序排序；（10分）
test2 = spark.sql(
    """
    
    SELECT year(data) as year,
            sum(Volume) as total_Volume
    FROM aaa
    GROUP BY year(data)
    ORDER BY total_Volume DESC
    """
)

# test2.show()

# 4）、业务2：编写SQL，统计每天开盘价和收盘价差值，找出差值最大前20；（15分）



test3 = spark.sql(
    """
    
    SELECT  data,
            round(Close - Open) AS diff_open_close
    FROM aaa
    ORDER BY diff_open_close DESC
    LIMIT 20
    """
)
test3.show()



# 5）、业务3：编写SQL，找出股票连续7天以上股票上涨日期区间，显示控制台；（15分）


# test4 = spark.sql(
#     """
#     WITH daily AS (
#     SELECT Date,+qwer
#            Close,
#            LAG(Close) OVER (ORDER BY Date) AS prev_close
#     FROM aaa
# ),
# flag AS (
#     SELECT Date,
#            CASE WHEN Close > prev_close THEN 0 ELSE 1 END AS is_up
#     FROM daily
# ),
# grp AS (
#     SELECT Date,
#            SUM(is_up) OVER (ORDER BY Date ROWS UNBOUNDED PRECEDING) AS grp_id
#     FROM flag
# ),
# consec AS (
#     SELECT grp_id,
#            MIN(Date) AS start_date,
#            MAX(Date) AS end_date,
#            COUNT(*)  AS days
#     FROM grp
#     GROUP BY grp_id
#     HAVING days >= 7
# )
# SELECT CONCAT('连续上涨区间：', start_date, ' 至 ', end_date, ' 共 ', days, ' 天') AS msg
# FROM consec
# ORDER BY start_date
#     """
# )
#
# test4.show()


# 6）、业务4：编写SQL，统计每天最高股票于最低股票差值，找出相差最大前20；（10分）
# 7）、业务5：编写SQL，找出2020年6月份至8月份每日票价波峰和波谷；（15分）
# 8）、业务5：编写SQL，对2021年交易数据，使用窗口函数，按照周统计交易量，并降序排序；（15分）