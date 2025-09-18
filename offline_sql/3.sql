create table ff(
    id string
)

-- select sku_id,
--        collect_set(
--            named_struct(
--                'attr_id',attr_id,
--                'value_id',value_id,
--                'attr_name',attr_name,
--                'value_name',value_name
--                )
--            ) as attrs
-- from bigdata_offline_v1_ws.ods_sku_attr_value
-- where dt = ${bizdate}
-- group by sku_id