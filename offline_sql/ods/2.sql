show databases ;

select *
from bigdata_offline_v1_ws.ods_order_info
left join bigdata_offline_v1_ws.ods_base_category3
left join bigdata_offline_v1_ws.ods_base_category2
left join bigdata_offline_v1_ws.ods_base_category1;


with sku_base as (
select id,
       spu_id,
       price,
       sku_name,
       sku_desc,
       weight,
       tm_id,
       category3_id,
       sku_default_img,
       is_sale,
       create_time,
       operate_time,
       dt
from bigdata_offline_v1_ws.ods_sku_info
where dt = ${bizdate}
),
    spu_base_t as (
        select id,
               spu_name
        from bigdata_offline_v1_ws.ods_spu_info
        where dt = ${bizdate}
    ),
     c3 as (
         select id,
                name,
                category2_id
         from bigdata_offline_v1_ws.ods_base_category3
         where dt = ${bizdate}
     ),
     c2 as (
         select id,
                name,
                category1_id
         from bigdata_offline_v1_ws.ods_base_category2
         where dt = ${bizdata}
     ),
      c1 as (
          select id,
                 name
          from bigdata_offline_v1_ws.ods_base_category1
          where dt = ${bizdate}
      ),
      tm as(
          select id,
                 tm_name
          from bigdata_offline_v1_ws.ods_base_trademark
          where dt = ${bizdate}
      ),
      attr as (
           select sku_id,
               collect_set(
               named_struct(
                   'attr_id',attr_id,
                   'value_id',value_id,
                   'attr_name',attr_name,
                   'value_name',value_name
                   )
               ) as attrs
        from bigdata_offline_v1_ws.ods_sku_attr_value
        where dt = ${bizdate}
        group by sku_id
      ),
      sale_attr as (
          select sku_id,
                 collect_set(
                      named_struct(
                          'sale_attr_id',sale_attr_id,
                          'sale_attr_value_id',sale_attr_value_id,
                          'sale_attr_name',sale_attr_name,
                          'sale_attr_value_name',sale_attr_value_name
                          )
                     ) as sale_attrs
          from bigdata_offline_v1_ws.ods_sku_sale_attr_value
          where dt = ${bizdate}
          group by sku_id
      )
select *
from c3







