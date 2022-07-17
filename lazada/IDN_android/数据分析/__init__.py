# pyspark \
# --driver-memory 10G \
# --executor-memory 50G \
# --num-executors 12 \
# --executor-cores 32
# df_1 = spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/pkg/lazada/IDN_android/sample_ana/20220701_delay_2")
# df_2 = spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/pkg/lazada/IDN_android/sample_ana/20220701_delay_2_delay")
# df_1.createTempView("df_1_table")
# df_2.createTempView("df_2_table")
#
# spark.sql("   \
# select  \
#     count(device_id) as device_id_cnt   \
# from    \
#     df_1_table  \
# where   \
#     now_label=0 \
#     and after_label=1   \
# ").show(500,truncate=False)
# # +-------------+
# # |device_id_cnt|
# # +-------------+
# # |20099        |
# # +-------------+
#
# spark.sql("   \
# select  \
#     lastdaygap_label,   \
#     count(device_id) as device_id_cnt   \
# from    \
#     df_1_table  \
# where   \
#     now_label=0 \
#     and after_label=1   \
# group by    \
#     lastdaygap_label    \
# order by    \
#     device_id_cnt desc  \
# ").show(500,truncate=False)
# # +----------------+-------------+
# # |lastdaygap_label|device_id_cnt|
# # +----------------+-------------+
# # |0               |14205        |
# # |1               |2121         |
# # |2               |1151         |
# # |3               |841          |
# # |4               |447          |
# # |5               |386          |
# # |6               |273          |
# # |7               |201          |
# # |8               |145          |
#
# spark.sql("\
#     select  \
#         bundle_app_set,   \
#         bundle_app_set_size \
#     from    \
#         df_1_table  \
#     where   \
#         bundle_app_set is null  \
# ").show(500,truncate=False)
#
# +--------------+-------------------+
# |bundle_app_set|bundle_app_set_size|
# +--------------+-------------------+
# +--------------+-------------------+
#
# spark.sql("   \
#     select  \
#         count(df_1_table.device_id) as device_id_cnt    \
#     from    \
#         df_1_table  \
#     left join   \
#         df_2_table  \
#     on  \
#         df_1_table.device_id = df_2_table.device_id \
#     where   \
#         df_2_table.delay_bundle_app_set_size >  df_1_table.bundle_app_set_size  \
# ").show(500,truncate=False)
#
# +-------------+
# |device_id_cnt|
# +-------------+
# |145375       |
# +-------------+
#
# spark.sql("   \
#     select  \
#         now_label,after_label,   \
#         count(df_1_table.device_id) as device_id_cnt    \
#     from    \
#         df_1_table  \
#     left join   \
#         df_2_table  \
#     on  \
#         df_1_table.device_id = df_2_table.device_id \
#     where   \
#         df_2_table.delay_bundle_app_set_size >  df_1_table.bundle_app_set_size  \
#     group by    \
#         now_label,after_label   \
#     order by    \
#         device_id_cnt desc  \
# ").show(500,truncate=False)
#
#
# # +---------+-----------+-------------+
# # |now_label|after_label|device_id_cnt|
# # +---------+-----------+-------------+
# # |0        |0          |118632       |
# # |0        |1          |20099        |
# # |1        |1          |6644         |
# # +---------+-----------+-------------+
#
# df_array_diff_ini = spark.sql("   \
#     select  \
#         df_1_table.device_id,   \
#         bundle_app_set,  \
#         delay_bundle_app_set,    \
#         bundle_app_set_size,    \
#         osversion   \
#     from    \
#         df_1_table  \
#     left join   \
#         df_2_table  \
#     on  \
#         df_1_table.device_id = df_2_table.device_id \
#     where   \
#         df_2_table.delay_bundle_app_set_size >  df_1_table.bundle_app_set_size  \
#         and now_label = 0   \
#         and after_label = 0 \
# ").persist()
# df_array_diff_ini.createTempView("df_array_diff_ini_table_2")
#
# spark.sql(" \
#     select  \
#         delay_bundle_app,   \
#         count(device_id) as device_id_cnt   \
#     from    \
#     (   \
#         select  \
#             t1.device_id    \
#             ,t1.delay_bundle_app    \
#         from    \
#         (   \
#             select  \
#                 device_id   \
#                 ,delay_bundle_app   \
#             from    \
#                 df_array_diff_ini_table_2 \
#             lateral view    \
#                 explode(delay_bundle_app_set) as delay_bundle_app   \
#         )t1 \
#         left join   \
#         (   \
#             select  \
#                 device_id   \
#                 ,bundle_app \
#             from    \
#                 df_array_diff_ini_table \
#             lateral view    \
#                 explode(bundle_app_set) as bundle_app   \
#         )t2 \
#         on  \
#             t1.device_id = t2.device_id \
#             and t1.delay_bundle_app = t2.bundle_app \
#         where   \
#             t2.bundle_app is null   \
#     )t3 \
#     group by    \
#         delay_bundle_app    \
#     order by    \
#         device_id_cnt desc  \
# ").show(500,truncate=False)
#
# # +--------------------------------------------------------------------------------------------------+-------------+
# # |delay_bundle_app                                                                                  |device_id_cnt|
# # +--------------------------------------------------------------------------------------------------+-------------+
# # |com.adfone.unefon                                                                                 |47374        |
# # |id.co.shopintar                                                                                   |39830        |
# # |com.app.tokobagus.betterb                                                                         |17825        |
# # |com.alibaba.aliexpresshd                                                                          |3379         |
# # |blibli.mobile.commerce                                                                            |2831         |
# # |com.adfone.indosat                                                                                |1938         |
# # |com.tycmrelx.cash                                                                                 |1917         |
# # |com.thecarousell.Carousell                                                                        |601          |
# # |diamond.via.id                                                                                    |581          |
# # |com.codmobil.mobilcod                                                                             |475          |
# # |com.alibaba.intl.android.apps.poseidon                                                            |417          |
# # |com.newchic.client                                                                                |180          |
# # |com.shopee.id                                                                                     |122          |
# # |com.tikshop.tikshop                                                                               |117          |
# # |com.propcoid.propcoid                                                                             |97           |
#
# spark.sql(" \
#     select  \
#         bundle_app_set_size,    \
#         count(device_id) as device_id_cnt   \
#     from    \
#         df_array_diff_ini_table_2 \
#     group by    \
#         bundle_app_set_size \
#     order by    \
#         device_id_cnt desc  \
# ").show(500,truncate=False)
#
# # +-------------------+-------------+
# # |bundle_app_set_size|device_id_cnt|
# # +-------------------+-------------+
# # |0                  |113472       |
# # |1                  |4699         |
# # |2                  |405          |
# # |3                  |51           |
# # |4                  |3            |
# # |6                  |1            |
# # |9                  |1            |
# # +-------------------+-------------+
#
# spark.sql(" \
#     select  \
#         osversion,    \
#         count(device_id) as device_id_cnt   \
#     from    \
#         df_array_diff_ini_table_2 \
#     group by    \
#         osversion \
#     order by    \
#         device_id_cnt desc  \
# ").show(500,truncate=False)
#
# # +---------------------+-------------+
# # |osversion            |device_id_cnt|
# # +---------------------+-------------+
# # |8.0                  |28581        |
# # |7.0                  |21021        |
# # |unknow               |13124        |
# # |9.0                  |11713        |
# # |11.0.0               |8697         |
# # |10.0                 |6981         |
# # |11.0                 |3638         |
# # |8.1.1                |3612         |
# # |null                 |1793         |
# # |6.0.1                |1757         |
# # |30                   |1680         |
# # |8.1.0                |1356         |
# # |4.0                  |1354         |
# # |5.1.1                |1220         |
# # |12                   |1157         |
# # |7.1.2                |1139         |
# # |12.0.0               |1087         |
#
# spark.sql(" \
#     select  \
#         osversion,   \
#         count(device_id) as device_id_cnt   \
#     from    \
#         df_1_table \
#     where   \
#         now_label=0 \
#         and after_label=1   \
#     group by    \
#         osversion \
#     order by    \
#         device_id_cnt  desc  \
# ").show(500,truncate=False)
#
# # +-------------+-------------+
# # |osversion    |device_id_cnt|
# # +-------------+-------------+
# # |11.0.0       |3922         |
# # |10.0         |2607         |
# # |11.0         |2509         |
# # |11           |2430         |
# # |9.0          |1921         |
# # |8.1.1        |982          |
# # |12           |783          |
# # |unknow       |724          |
# # |8.1.0        |517          |
# # |12.0.0       |408          |
# # |2.3          |369          |
# # |4.0          |332          |
# # |6.0.1        |255          |
# # |8.0          |244          |
# # |N            |218          |
# # |7.1.2        |214          |
# # |30           |164          |
# # |5.1.1        |158          |
# # |5.1          |143          |
# # |30.0         |133          |
# # |null         |122          |
# # |8.1          |116          |
# # |12.0         |114          |
# # |7.1.1        |105          |
# # |6.0          |99           |
# # |7.0          |96           |
# # |4.4.4        |78           |
# # |5.0.2        |64           |
# # |30(11)       |42           |
# # |4.4.2        |38           |
# # |29.0         |27           |
# # |31           |24           |
# # |5.0          |24           |
# # |31.0         |21           |
# # |Unknown      |20           |
# # |29(10)       |11           |
# # |5.x          |10           |
# # |Android_Other|8            |
# # |31(12)       |6            |
# # |5.80.8       |5            |
# # |7.1          |5            |
# # |9.null       |4            |
# # |5.79.8       |4            |
# # |Android_8.x  |2            |
# # |11.0.2201117 |2            |
# # |4.2.2        |2            |
# # |10.null      |2            |
# # |4.0.3        |2            |
# # |5.78.14      |2            |
# # |11.0.2201116 |1            |
# # |3.0          |1            |
# # |32.0         |1            |
# # |Android_7.x  |1            |
# # |5.0.1        |1            |
# # |32(12)       |1            |
# # |9.1.0        |1            |
# # |Android_4.x  |1            |
# # |12.null      |1            |
# # |4.1          |1            |
# # |11.0.220333  |1            |
# # +-------------+-------------+