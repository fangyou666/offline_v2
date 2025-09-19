DROP TABLE IF EXISTS ads_traffic_stats_by_channel;
CREATE EXTERNAL TABLE ads_traffic_stats_by_channel
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `channel`          STRING COMMENT '渠道',
    `uv_count`         BIGINT COMMENT '访客人数',
    `avg_duration_sec` BIGINT COMMENT '会话平均停留时长，单位为秒',
    `avg_page_count`   BIGINT COMMENT '会话平均浏览页面数',
    `sv_count`         BIGINT COMMENT '会话数',
    `bounce_rate`      DECIMAL(16, 2) COMMENT '跳出率'
) COMMENT '各渠道流量统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_traffic_stats_by_channel/';


-- insert overwrite table ads_traffic_stats_by_channel
-- select * from ads_traffic_stats_by_channel
-- union
-- select
--     '20250916' dt,
--     recent_days,
--     channel,
--     cast(count(distinct(mid_id)) as bigint) uv_count,
--     cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
--     cast(avg(page_count_1d) as bigint) avg_page_count,
--     cast(count(*) as bigint) sv_count,
--     cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
-- from dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
-- where dt>=date_add('20250916',-recent_days+1)
-- group by recent_days,channel;


DROP TABLE IF EXISTS ads_page_path;
CREATE EXTERNAL TABLE ads_page_path
(
    `dt`          STRING COMMENT '统计日期',
    `source`      STRING COMMENT '跳转起始页面ID',
    `target`      STRING COMMENT '跳转终到页面ID',
    `path_count`  BIGINT COMMENT '跳转次数'
) COMMENT '页面浏览路径分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_page_path/';


-- insert overwrite table ads_page_path
-- select * from ads_page_path
-- union
-- select
--     20250916 dt,
--     source,
--     nvl(target,'null'),
--     count(*) path_count
-- from
--     (
--         select
--             concat('step-',rn,':',page_id) source,
--             concat('step-',rn+1,':',next_page_id) target
--         from
--             (
--                 select
--                     page_id,
--                     lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
--                     row_number() over (partition by session_id order by view_time) rn
--                 from dwd_traffic_page_view_inc
--                 where dt=20250916
--             )t1
--     )t2
-- group by source,target;


DROP TABLE IF EXISTS ads_user_change;
CREATE EXTERNAL TABLE ads_user_change
(
    `dt`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_change/';

--
-- insert overwrite table ads_user_change
-- select * from ads_user_change
-- union
-- select
--     churn.dt,
--     user_churn_count,
--     user_back_count
-- from
--     (
--         select
--             20250916 dt,
--             count(*) user_churn_count
--         from dws_user_user_login_td
--         where dt=20250916
--           and login_date_last=date_add(20250916,-7)
--     )churn
--         join
--     (
--         select
--             20250916 dt,
--             count(*) user_back_count
--         from
--             (
--                 select
--                     user_id,
--                     login_date_last
--                 from dws_user_user_login_td
--                 where dt=20250916
--                   and login_date_last = '2022-06-08'
--             )t1
--                 join
--             (
--                 select
--                     user_id,
--                     login_date_last login_date_previous
--                 from dws_user_user_login_td
--                 where dt=date_add(20250916,-1)
--             )t2
--             on t1.user_id=t2.user_id
--         where datediff(login_date_last,login_date_previous)>=8
--     )back
--     on churn.dt=back.dt;

DROP TABLE IF EXISTS ads_user_retention;
CREATE EXTERNAL TABLE ads_user_retention
(
    `dt`              STRING COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
) COMMENT '用户留存率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_retention/';


insert overwrite table ads_user_retention
select * from ads_user_retention
union
select 20250916 dt,
       login_date_first create_date,
       datediff(20250916, login_date_first) retention_day,
       sum(if(login_date_last = 20250916, 1, 0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last = 20250916, 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                login_date_last,
                login_date_first
         from dws_user_user_login_td
         where dt = 20250916
           and login_date_first >= date_add(20250916, -7)
           and login_date_first < 20250916
     ) t1
group by login_date_first;

DROP TABLE IF EXISTS ads_user_stats;
CREATE EXTERNAL TABLE ads_user_stats
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    BIGINT COMMENT '新增用户数',
    `active_user_count` BIGINT COMMENT '活跃用户数'
) COMMENT '用户新增活跃统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_stats/';


insert overwrite table ads_user_stats
select * from ads_user_stats
union
select '2025-09-16' dt,
       recent_days,
       sum(if(login_date_first >= date_add('2025-09-16', -recent_days + 1), 1, 0)) new_user_count,
       count(*) active_user_count
from dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt = '2025-09-16'
  and login_date_last >= date_add('2025-09-16', -recent_days + 1)
group by recent_days;

DROP TABLE IF EXISTS ads_user_action;
CREATE EXTERNAL TABLE ads_user_action
(
    `dt`                STRING COMMENT '统计日期',
    `home_count`        BIGINT COMMENT '浏览首页人数',
    `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
    `cart_count`        BIGINT COMMENT '加购人数',
    `order_count`       BIGINT COMMENT '下单人数',
    `payment_count`     BIGINT COMMENT '支付人数'
) COMMENT '用户行为漏斗分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_action/';



insert overwrite table ads_user_action
select * from ads_user_action
union
select
    '20250916' dt,  -- 改为纯数字格式，与源表保持一致
    home_count,
    good_detail_count,
    cart_count,
    order_count,
    payment_count
from
    (
        select
            1 recent_days,
            sum(if(page_id='home',1,0)) home_count,
            sum(if(page_id='good_detail',1,0)) good_detail_count
        from dws_traffic_page_visitor_page_view_1d
        where dt='20250916'  -- 这里也需要同步修改
          and page_id in ('home','good_detail')
    )page
        join
    (
        select
            1 recent_days,
            count(*) cart_count
        from dws_trade_user_cart_add_1d
        where dt='20250916'  -- 这里也需要同步修改
    )cart
    on page.recent_days=cart.recent_days
        join
    (
        select
            1 recent_days,
            count(*) order_count
        from dws_trade_user_order_1d
        where dt='20250916'  -- 这里也需要同步修改
    )ord
    on page.recent_days=ord.recent_days
        join
    (
        select
            1 recent_days,
            count(*) payment_count
        from dws_trade_user_payment_1d
        where dt='20250916'  -- 这里也需要同步修改
    )pay
    on page.recent_days=pay.recent_days;



DROP TABLE IF EXISTS ads_new_order_user_stats;
CREATE EXTERNAL TABLE ads_new_order_user_stats
(
    `recent_days`          string COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `new_order_user_count` string COMMENT '新增下单人数'
) COMMENT '新增下单用户统计'
    PARTITIONED BY (dt STRING COMMENT '统计日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_new_order_user_stats/';


DROP TABLE IF EXISTS ads_order_continuously_user_count;
CREATE EXTERNAL TABLE ads_order_continuously_user_count
(
    `dt`                            STRING COMMENT '统计日期',
    `recent_days`                   BIGINT COMMENT '最近天数,7:最近7天',
    `order_continuously_user_count` BIGINT COMMENT '连续3日下单用户数'
) COMMENT '最近7日内连续3日下单用户数统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_continuously_user_count/';



