

df=spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample_ana/20220707_delay_2/")
df.createTempView("df_table")
spark.sql("desc df_table").show(100,truncate=False)
"""
+----------------------+-------------+-------+
|col_name              |data_type    |comment|
+----------------------+-------------+-------+
|device_id             |string       |null   |
|osversion             |string       |null   |
|lastactiveday         |string       |null   |
|activedays            |array<int>   |null   |
|lastdaygap_label      |int          |null   |
|bundles_size_label    |int          |null   |
|now_label             |int          |null   |
|now_app_set_size      |int          |null   |
|now_target_app_label  |string       |null   |
|after_label           |int          |null   |
|after_app_set_size    |int          |null   |
|after_target_app_label|string       |null   |
|activeday_label_1_cnt |bigint       |null   |
|activeday_label_2_cnt |bigint       |null   |
|activeday_label_3_cnt |bigint       |null   |
|activeday_label_4_cnt |bigint       |null   |
|activeday_label_5_cnt |bigint       |null   |
|activeday_label_6_cnt |bigint       |null   |
|activeday_label_7_cnt |bigint       |null   |
|activeday_label_8_cnt |bigint       |null   |
|activeday_label_9_cnt |bigint       |null   |
|activeday_label_10_cnt|bigint       |null   |
|activeday_label_11_cnt|bigint       |null   |
|activeday_label_12_cnt|bigint       |null   |
|activeday_label_13_cnt|bigint       |null   |
|activeday_label_14_cnt|bigint       |null   |
|activeday_label_15_cnt|bigint       |null   |
|activeday_label_16_cnt|bigint       |null   |
|activeday_label_17_cnt|bigint       |null   |
|activeday_label_18_cnt|bigint       |null   |
|activeday_label_19_cnt|bigint       |null   |
|activeday_label_20_cnt|bigint       |null   |
|activeday_label_21_cnt|bigint       |null   |
|activeday_label_22_cnt|bigint       |null   |
|activeday_label_23_cnt|bigint       |null   |
|activeday_label_24_cnt|bigint       |null   |
|activeday_label_25_cnt|bigint       |null   |
|activeday_label_26_cnt|bigint       |null   |
|activeday_label_27_cnt|bigint       |null   |
|activeday_label_28_cnt|bigint       |null   |
|activeday_label_29_cnt|bigint       |null   |
|activeday_label_30_cnt|bigint       |null   |
|activeday_label_1_01  |int          |null   |
|activeday_label_2_01  |int          |null   |
|activeday_label_3_01  |int          |null   |
|activeday_label_4_01  |int          |null   |
|activeday_label_5_01  |int          |null   |
|activeday_label_6_01  |int          |null   |
|activeday_label_7_01  |int          |null   |
|activeday_label_8_01  |int          |null   |
|activeday_label_9_01  |int          |null   |
|activeday_label_10_01 |int          |null   |
|activeday_label_11_01 |int          |null   |
|activeday_label_12_01 |int          |null   |
|activeday_label_13_01 |int          |null   |
|activeday_label_14_01 |int          |null   |
|activeday_label_15_01 |int          |null   |
|activeday_label_16_01 |int          |null   |
|activeday_label_17_01 |int          |null   |
|activeday_label_18_01 |int          |null   |
|activeday_label_19_01 |int          |null   |
|activeday_label_20_01 |int          |null   |
|activeday_label_21_01 |int          |null   |
|activeday_label_22_01 |int          |null   |
|activeday_label_23_01 |int          |null   |
|activeday_label_24_01 |int          |null   |
|activeday_label_25_01 |int          |null   |
|activeday_label_26_01 |int          |null   |
|activeday_label_27_01 |int          |null   |
|activeday_label_28_01 |int          |null   |
|activeday_label_29_01 |int          |null   |
|activeday_label_30_01 |int          |null   |
|after_app_set         |array<string>|null   |
|now_app_set           |array<string>|null   |
+----------------------+-------------+-------+
"""
spark.sql("select now_label,after_label,count(device_id) as device_id_cnt from df_table group by now_label,after_label order by device_id_cnt desc").show(100,truncate=False)
"""
+---------+-----------+-------------+                                           
|now_label|after_label|device_id_cnt|
+---------+-----------+-------------+
|0        |0          |174600474    |
|1        |1          |2469311      |
|0        |1          |37263        |
|1        |0          |10186        |
+---------+-----------+-------------+
"""

spark.sql(" \
select  \
    now_app_set_size \
    ,count(device_id) as device_id_cnt  \
from    \
(   \
    select  \
        device_id   \
        ,now_app_set_size    \
    from    \
        df_table    \
    where   \
        now_label=0 \
        and after_label=1   \
)t2 \
group by    \
    now_app_set_size \
order by    \
    device_id_cnt desc").show(100,truncate=False)
"""
+----------------+-------------+                                                
|now_app_set_size|device_id_cnt|
+----------------+-------------+
|0               |30561        |
|1               |4561         |
|2               |1106         |
|3               |535          |
|4               |250          |
|5               |117          |
|6               |66           |
|7               |32           |
|8               |11           |
|11              |6            |
|9               |6            |
|10              |5            |
|12              |4            |
|13              |2            |
|14              |1            |
+----------------+-------------+
"""

spark.sql("   \
    select  \
        label_temp  \
        ,count(device_id) as device_id_cnt  \
    from    \
    (   \
        select  \
            device_id   \
            ,case   \
            when after_label=1 then 1   \
            when after_label=0 and after_app_set_size > now_app_set_size then 2 \
            else 3 end as label_temp    \
        from    \
            df_table    \
        where   \
            now_label=0 \
    )t1 \
    group by    \
        label_temp  \
    order by    \
        device_id_cnt desc  \
").show(100,truncate=False)
"""
+----------+-------------+                                                      
|label_temp|device_id_cnt|
+----------+-------------+
|3         |174067191    |
|2         |533283       |
|1         |37263        |
+----------+-------------+
"""

spark.sql(" \
    select  \
        now_app_set_size   \
        ,count(device_id) as device_id_cnt  \
    from    \
    (   \
        select  \
            t2_after.device_id  \
            ,t2_after.after_app   \
            ,now_app_set_size  \
        from    \
        (   \
            select  \
                device_id   \
                ,now_app    \
            from    \
            (   \
                select  \
                    now_app_set \
                    ,device_id  \
                from    \
                    df_table    \
                where   \
                    now_label=0 \
                    and after_label=0   \
                    and after_app_set_size > now_app_set_size   \
            )t1_now \
            lateral view    \
                explode(now_app_set) as now_app \
        )t2_now \
        right join  \
        (   \
            select  \
                device_id   \
                ,after_app  \
                ,now_app_set_size   \
            from    \
            (   \
                select  \
                    after_app_set   \
                    ,device_id  \
                    ,now_app_set_size   \
                from    \
                    df_table    \
                where   \
                    now_label=0 \
                    and after_label=0   \
                    and after_app_set_size > now_app_set_size   \
            )t1_after   \
            lateral view    \
                explode(after_app_set) as after_app   \
        )t2_after   \
        on  \
            t2_now.device_id = t2_after.device_id   \
            and t2_now.now_app = t2_after.after_app  \
        where   \
            t2_now.now_app is null  \
    )t3 \
    group by    \
        now_app_set_size   \
    order by    \
        device_id_cnt desc  \
").show(100,truncate=False)

"""
+----------------+-------------+                                                
|now_app_set_size|device_id_cnt|
+----------------+-------------+
|0               |258282       |
|1               |115443       |
|2               |65072        |
|3               |40607        |
|4               |24880        |
|5               |15067        |
|6               |9139         |
|7               |5555         |
|8               |3499         |
|9               |2112         |
|10              |1352         |
|11              |920          |
|12              |574          |
|13              |411          |
|14              |247          |
|15              |196          |
|16              |139          |
|17              |102          |
|18              |84           |
|19              |78           |
|21              |52           |
|22              |48           |
|20              |44           |
|23              |31           |
|24              |27           |
|25              |19           |
|26              |16           |
|27              |13           |
|35              |12           |
|28              |10           |
|30              |8            |
|32              |7            |
|31              |7            |
|37              |6            |
|36              |5            |
|38              |5            |
|54              |5            |
|33              |5            |
|29              |5            |
|34              |4            |
|131             |3            |
|69              |3            |
|48              |3            |
|45              |3            |
|44              |3            |
|50              |3            |
|41              |3            |
|64              |2            |
|71              |2            |
|39              |2            |
|43              |2            |
|49              |2            |
|56              |2            |
|66              |1            |
|58              |1            |
|70              |1            |
|47              |1            |
|46              |1            |
|67              |1            |
|96              |1            |
|84              |1            |
|40              |1            |
|126             |1            |
|52              |1            |
|53              |1            |
+----------------+-------------+
"""