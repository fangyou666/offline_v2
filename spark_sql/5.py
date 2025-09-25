from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("five") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df1 = (spark.read.text("hdfs://cdh01:8020/user/hive/warehouse/bigdata_offline_v1_ws.db/goods")
.selectExpr(
    "split(value,',')[0] as goods_id",
    "split(value,',')[1] as order_id",
    "split(value,',')[2] as goods_name",
    "split(value,',')[3] as weight",
    "split(value,',')[4] as volume",
    "split(value,',')[5] as category"))
df1.createOrReplaceTempView("goods")

test1 = spark.sql("select * from goods")
# test1.show()


df2 = (spark.read.text("hdfs://cdh01:8020/user/hive/warehouse/bigdata_offline_v1_ws.db/logistics")
.selectExpr(

    "split(value,',')[0] as logistics_id",
    "split(value,',')[1] as order_id",
    "split(value,',')[2] as express_company",
    "split(value,',')[3] as departure_time",
    "split(value,',')[4] as arrival_time",
    "split(value,',')[5] as current_city",
    "split(value,',')[6] as logistics_status"))
df2.createOrReplaceTempView("logistics")

test2 = spark.sql("select * from logistics")
# test2.show()


df3 = (spark.read.text("hdfs://cdh01:8020/user/hive/warehouse/bigdata_offline_v1_ws.db/orders")
.selectExpr(

    "split(value,',')[0] as order_id",
    "split(value,',')[1] as user_id",
    "split(value,',')[2] as order_time",
    "split(value,',')[3] as total_amount",
    "split(value,',')[4] as status",
    "split(value,',')[5] as origin",
    "split(value,',')[6] as destination "))

df3.createOrReplaceTempView("orders")

test3 = spark.sql("select * from orders")
# test3.show()


df4 = (spark.read.text("hdfs://cdh01:8020/user/hive/warehouse/bigdata_offline_v1_ws.db/users")
.selectExpr(

    "split(value,',')[0] as user_id",
    "split(value,',')[1] as user_name",
    "split(value,',')[2] as register_time",
    "split(value,',')[3] as user_level",
    "split(value,',')[4] as city"

))
df4.createOrReplaceTempView("users")

test4 = spark.sql("select * from users")
# test4.show()

# 1.统计每个城市的订单数量，按订单数降序排列

d1 = spark.sql(

    """
    
    SELECT destination,count(*) as cnt
    FROM orders
    GROUP BY destination
    ORDER BY cnt desc
    
    """

)

# d1.show()

# 2.计算 2023 年每个月的订单总金额，并筛选出月总金额超过 100 万的月份

d2 = spark.sql(

    """
    
    SELECT date_format(order_time,'yyyy-MM') month,
    round(sum(total_amount),2) as amount
    FROM orders
    where year(order_time)=2023
    GROUP BY date_format(order_time,'yyyy-MM')
    having amount > 1000000
    ORDER BY month
    
    """

)

# d2.show()

# 3. 发货时间比下单时间晚超过 24 小时的订单
d3 = spark.sql(

    """

    SELECT o.order_id,
           o.order_time,
           l.departure_time
    FROM orders o
    JOIN logistics l
    ON o.order_id = l.order_id
    WHERE l.departure_time > o.order_time + INTERVAL 24 HOURS

    """

)

# d3.show()

# 4.统计各快递公司的物流单量占比（保留两位小数）

d4 = spark.sql(

    """

    SELECT express_company,
           ROUND(count(*) * 100.0 / sum(count(*)) OVER () ,2) pct
    FROM logistics
    GROUP BY express_company
    ORDER BY pct desc
    
    """

)

# d4.show()


# 5.计算每个订单的物流运输时长（到达时间 - 发货时间），并找出运输时长最长的前 10 个订单

d5 = spark.sql(

    """

    select  o.order_id,
            ROUND((UNIX_TIMESTAMP(arrival_time) - UNIX_TIMESTAMP(departure_time))/3600,2) sc
    from orders o
             join logistics l on o.order_id = l.order_id
    WHERE l.arrival_time IS NOT NULL
    order by sc desc limit 10;
    
    """

)

# d5.show()

# 6.筛选出 "黄金" 和 "钻石" 等级用户的所有订单，要求订单状态为 "已签收"


d6 = spark.sql(

    """

    SELECT *
    FROM users u
    join orders o on o.user_id = u.user_id
    WHERE user_level in ('黄金','钻石') and o.status='已签收'  
    
    """

)

d6.show()


# 7.统计每种货物类别的总重量和总体积


# d7 = spark.sql(
#
#     """
#
#     SELECT goods_name,sum() as
#     FROM goods
#     GROUP BY goods_name
#
#     """
#
# )
#
# d7.show()


# 8.找出从 "北京" 发往 "上海" 的所有订单中，运输时长超过 48 小时的物流信息


# 9.计算每个用户的平均订单金额（只统计已完成的订单）


# 10.统计每天各时段（0-6 点、7-12 点、13-18 点、19-23 点）的下单量


# 11.找出所有物流状态为 "在途" 但超过 72 小时未更新的物流单


# 12.计算每个城市作为发货地的平均订单金额


# 13.关联订单表和物流表，找出所有发货时间比下单时间晚超过 24 小时的订单


# 14.统计每个用户的订单总数，筛选出订单数超过 10 的用户


# 15.按季度统计各快递公司的平均运输时长


# 16.找出所有包含 "电子" 类货物且订单金额超过 5000 元的订单信息


# 17.计算每个城市的订单取消率（取消订单数 / 总订单数）


# 18.统计 2023 年第四季度各货物类别的订单数量


# 19.找出所有重复下单的用户（同一天内下单次数≥2 的用户）


# 20.关联订单表、物流表和用户表，计算各用户等级的平均运输时长
