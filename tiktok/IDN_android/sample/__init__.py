# pyspark \
# --driver-memory 10G \
# --executor-memory 50G \
# --num-executors 12 \
# --executor-cores 32
#
# sample_date="20220630"
# df=spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/pkg/lazada/IDN_android/sample_ana/%s_delay_2"%sample_date)
# df_fill=df.fillna(0,subset=["after_app_set_size"]).fillna(0,subset=["now_app_set_size"]).fillna(0,subset=["now_label"]).fillna(0,subset=["after_label"])
# df_fill.createTempView("%s_table"%sample_date)
# df_sample=spark.sql("   \
#     select  \
#         device_id, osversion    \
#         , activeday_label_1_cnt, activeday_label_2_cnt, activeday_label_3_cnt, activeday_label_4_cnt, activeday_label_5_cnt, activeday_label_6_cnt, activeday_label_7_cnt, activeday_label_8_cnt, activeday_label_9_cnt, activeday_label_10_cnt \
#         , activeday_label_1_01, activeday_label_2_01, activeday_label_3_01, activeday_label_4_01, activeday_label_5_01, activeday_label_6_01, activeday_label_7_01, activeday_label_8_01, activeday_label_9_01, activeday_label_10_01   \
#         , activeday_label_11_cnt, activeday_label_12_cnt, activeday_label_13_cnt, activeday_label_14_cnt, activeday_label_15_cnt, activeday_label_16_cnt, activeday_label_17_cnt, activeday_label_18_cnt, activeday_label_19_cnt, activeday_label_20_cnt    \
#         , activeday_label_11_01, activeday_label_12_01, activeday_label_13_01, activeday_label_14_01, activeday_label_15_01, activeday_label_16_01, activeday_label_17_01, activeday_label_18_01, activeday_label_19_01, activeday_label_20_01  \
#         , activeday_label_21_cnt, activeday_label_22_cnt, activeday_label_23_cnt, activeday_label_24_cnt, activeday_label_25_cnt, activeday_label_26_cnt, activeday_label_27_cnt, activeday_label_28_cnt, activeday_label_29_cnt, activeday_label_30_cnt    \
#         , activeday_label_21_01, activeday_label_22_01, activeday_label_23_01, activeday_label_24_01, activeday_label_25_01, activeday_label_26_01, activeday_label_27_01, activeday_label_28_01, activeday_label_29_01, activeday_label_30_01  \
#         , bundles_size_label    \
#         , lastdaygap_label  \
#         ,case   \
#         when after_label=1 then 1   \
#         when after_label=0 and after_app_set_size > now_app_set_size then 2 \
#         else 3 end as label_temp    \
#     from    \
#         %s_table    \
#     where   \
#         lastdaygap_label <= 3   \
#         and now_app_set_size = 0    \
#         and now_label = 0"%sample_date)
# df_sample.repartition(200).write.mode("overwrite").orc("oss://sdkemr-yeahmobi/user/chensheng/pkg/lazada/IDN_android/sample/%s_delay_2_lastdaygap_3"%sample_date)
