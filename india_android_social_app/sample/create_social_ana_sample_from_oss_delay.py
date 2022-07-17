# -*- coding: utf-8 -*-
import argparse
import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import datetime
import warnings
warnings.filterwarnings('ignore')
import os
def create_bundle_social_set(pos_data_temp_table,df_app_info_table):
    print("对表%s构建 bundle_social_set、bundle_social_set_size 特征"%pos_data_temp_table)
    pos_data_bundle_social_set = spark.sql("   \
        select  \
                device_id   \
                ,collect_set(bundle) as bundle_social_set \
                ,size(collect_set(bundle)) as bundle_social_set_size \
            from    \
            (   \
                select  \
                    device_id   \
                    ,bundle \
                from    \
                (   \
                    select  \
                        device_id   \
                        ,bundle \
                    from    \
                        %s \
                    lateral view    \
                        explode(bundles) as bundle  \
                ) t1    \
                left join   \
                    %s t_app_info_table  \
                on  \
                    t1.bundle = t_app_info_table.id \
                where   \
                    subcategory_name='Social'   \
            ) t2    \
            group by    \
                device_id" % (pos_data_temp_table, df_app_info_table))

    return pos_data_bundle_social_set
def create_activeday_label_cnt(spark,pos_data_temp_table):
    print("对表%s构建 activeday_label_1_cnt...activeday_label_30_cnt,activeday_label_1_01...activeday_label_30_01特征 "%pos_data_temp_table)
    sql = "select  \
                device_id \
                ,count(if(activeday_label=1,device_id,null)) as activeday_label_1_cnt \
                ,count(if(activeday_label=2,device_id,null)) as activeday_label_2_cnt \
                ,count(if(activeday_label=3,device_id,null)) as activeday_label_3_cnt \
                ,count(if(activeday_label=4,device_id,null)) as activeday_label_4_cnt \
                ,count(if(activeday_label=5,device_id,null)) as activeday_label_5_cnt \
                ,count(if(activeday_label=6,device_id,null)) as activeday_label_6_cnt \
                ,count(if(activeday_label=7,device_id,null)) as activeday_label_7_cnt \
                ,count(if(activeday_label=8,device_id,null)) as activeday_label_8_cnt \
                ,count(if(activeday_label=9,device_id,null)) as activeday_label_9_cnt \
                ,count(if(activeday_label=10,device_id,null)) as activeday_label_10_cnt \
                ,count(if(activeday_label=11,device_id,null)) as activeday_label_11_cnt \
                ,count(if(activeday_label=12,device_id,null)) as activeday_label_12_cnt \
                ,count(if(activeday_label=13,device_id,null)) as activeday_label_13_cnt \
                ,count(if(activeday_label=14,device_id,null)) as activeday_label_14_cnt \
                ,count(if(activeday_label=15,device_id,null)) as activeday_label_15_cnt \
                ,count(if(activeday_label=16,device_id,null)) as activeday_label_16_cnt \
                ,count(if(activeday_label=17,device_id,null)) as activeday_label_17_cnt \
                ,count(if(activeday_label=18,device_id,null)) as activeday_label_18_cnt \
                ,count(if(activeday_label=19,device_id,null)) as activeday_label_19_cnt \
                ,count(if(activeday_label=20,device_id,null)) as activeday_label_20_cnt \
                ,count(if(activeday_label=21,device_id,null)) as activeday_label_21_cnt \
                ,count(if(activeday_label=22,device_id,null)) as activeday_label_22_cnt \
                ,count(if(activeday_label=23,device_id,null)) as activeday_label_23_cnt \
                ,count(if(activeday_label=24,device_id,null)) as activeday_label_24_cnt \
                ,count(if(activeday_label=25,device_id,null)) as activeday_label_25_cnt \
                ,count(if(activeday_label=26,device_id,null)) as activeday_label_26_cnt \
                ,count(if(activeday_label=27,device_id,null)) as activeday_label_27_cnt \
                ,count(if(activeday_label=28,device_id,null)) as activeday_label_28_cnt \
                ,count(if(activeday_label=29,device_id,null)) as activeday_label_29_cnt \
                ,count(if(activeday_label=30,device_id,null)) as activeday_label_30_cnt \
                ,if(count(if(activeday_label=1,device_id,null))>0,1,0) as activeday_label_1_01 \
                ,if(count(if(activeday_label=2,device_id,null))>0,1,0) as activeday_label_2_01 \
                ,if(count(if(activeday_label=3,device_id,null))>0,1,0) as activeday_label_3_01 \
                ,if(count(if(activeday_label=4,device_id,null))>0,1,0) as activeday_label_4_01 \
                ,if(count(if(activeday_label=5,device_id,null))>0,1,0) as activeday_label_5_01 \
                ,if(count(if(activeday_label=6,device_id,null))>0,1,0) as activeday_label_6_01 \
                ,if(count(if(activeday_label=7,device_id,null))>0,1,0) as activeday_label_7_01 \
                ,if(count(if(activeday_label=8,device_id,null))>0,1,0) as activeday_label_8_01 \
                ,if(count(if(activeday_label=9,device_id,null))>0,1,0) as activeday_label_9_01 \
                ,if(count(if(activeday_label=10,device_id,null))>0,1,0) as activeday_label_10_01 \
                ,if(count(if(activeday_label=11,device_id,null))>0,1,0) as activeday_label_11_01 \
                ,if(count(if(activeday_label=12,device_id,null))>0,1,0) as activeday_label_12_01 \
                ,if(count(if(activeday_label=13,device_id,null))>0,1,0) as activeday_label_13_01 \
                ,if(count(if(activeday_label=14,device_id,null))>0,1,0) as activeday_label_14_01 \
                ,if(count(if(activeday_label=15,device_id,null))>0,1,0) as activeday_label_15_01 \
                ,if(count(if(activeday_label=16,device_id,null))>0,1,0) as activeday_label_16_01 \
                ,if(count(if(activeday_label=17,device_id,null))>0,1,0) as activeday_label_17_01 \
                ,if(count(if(activeday_label=18,device_id,null))>0,1,0) as activeday_label_18_01 \
                ,if(count(if(activeday_label=19,device_id,null))>0,1,0) as activeday_label_19_01 \
                ,if(count(if(activeday_label=20,device_id,null))>0,1,0) as activeday_label_20_01 \
                ,if(count(if(activeday_label=21,device_id,null))>0,1,0) as activeday_label_21_01 \
                ,if(count(if(activeday_label=22,device_id,null))>0,1,0) as activeday_label_22_01 \
                ,if(count(if(activeday_label=23,device_id,null))>0,1,0) as activeday_label_23_01 \
                ,if(count(if(activeday_label=24,device_id,null))>0,1,0) as activeday_label_24_01 \
                ,if(count(if(activeday_label=25,device_id,null))>0,1,0) as activeday_label_25_01 \
                ,if(count(if(activeday_label=26,device_id,null))>0,1,0) as activeday_label_26_01 \
                ,if(count(if(activeday_label=27,device_id,null))>0,1,0) as activeday_label_27_01 \
                ,if(count(if(activeday_label=28,device_id,null))>0,1,0) as activeday_label_28_01 \
                ,if(count(if(activeday_label=29,device_id,null))>0,1,0) as activeday_label_29_01 \
                ,if(count(if(activeday_label=30,device_id,null))>0,1,0) as activeday_label_30_01 \
            from    \
            (   \
                select  \
                    device_id \
                    ,case \
                    when activeday <= 0 then 1 \
                    when activeday <= 1 then 2 \
                    when activeday <= 2 then 3 \
                    when activeday <= 3 then 4 \
                    when activeday <= 4 then 5 \
                    when activeday <= 5 then 6 \
                    when activeday <= 6 then 7 \
                    when activeday <= 7 then 8 \
                    when activeday <= 8 then 9 \
                    when activeday <= 9 then 10 \
                    when activeday <= 10 then 11 \
                    when activeday <= 12 then 12 \
                    when activeday <= 14 then 13 \
                    when activeday <= 16 then 14 \
                    when activeday <= 18 then 15 \
                    when activeday <= 20 then 16 \
                    when activeday <= 25 then 17 \
                    when activeday <= 30 then 18 \
                    when activeday <= 35 then 19 \
                    when activeday <= 40 then 20 \
                    when activeday <= 50 then 21 \
                    when activeday <= 60 then 22 \
                    when activeday <= 70 then 23 \
                    when activeday <= 80 then 24 \
                    when activeday <= 90 then 25 \
                    when activeday <= 100 then 26 \
                    when activeday <= 150 then 27 \
                    when activeday <= 200 then 28 \
                    when activeday <= 300 then 29 \
                    else 30 \
                    end as activeday_label \
                from    \
                    %s \
                lateral view    \
                    explode(activedays) as activeday    \
            ) t1    \
            group by    \
                device_id"%pos_data_temp_table
    pos_data_activeday_label_cnt = spark.sql(sql)
    return pos_data_activeday_label_cnt
def create_bundles_size_label(spark,pos_data_temp_table):
    print("对表%s构建 bundles_size_label特征"%pos_data_temp_table)
    pos_data_bundles_size_label = spark.sql(" \
        select  \
            device_id   \
            ,if(size(bundles)>=100,100,size(bundles)) as bundles_size_label \
        from    \
            %s  \
    "%pos_data_temp_table)
    return pos_data_bundles_size_label
def create_lastdaygap_label(spark,pos_data_temp_table):
    print("对表%s构建 lastdaygap_label特征"%pos_data_temp_table)
    pos_data_lastdaygap_label = spark.sql("  \
        select  \
            device_id   \
            ,if(activedays[size(activedays)-1]>=100,100,activedays[size(activedays)-1]) as  lastdaygap_label   \
        from    \
            %s  \
    "%pos_data_temp_table)
    return pos_data_lastdaygap_label
def get_pos_data_ini(spark,df_user_bundle_sample_date_after_table,df_user_bundle_sample_date_table):
    pos_data = spark.sql(" \
                 select \
                    t2.device_id \
                    ,bundles \
                    ,osversion \
                    ,lastactiveday \
                    ,activedays \
                    ,t1.target_app_label \
                from \
                ( \
                    select \
                        ifa as device_id \
                        ,case \
                         when array_contains(bundles,'com.next.innovation.takatak') then 1 \
                         when array_contains(bundles,'in.mohalla.video') then 2 \
                         when array_contains(bundles,'in.mohalla.sharechat') then 3 \
                         else 0 \
                         end as target_app_label \
                    from \
                        %s \
                    where \
                        array_contains(bundles,'com.next.innovation.takatak') or array_contains(bundles,'in.mohalla.video') or array_contains(bundles,'in.mohalla.sharechat') \
                ) t1 \
                right join \
                ( \
                    select \
                        ifa as device_id \
                        ,bundles \
                        ,osversion \
                        ,lastactiveday \
                        ,activedays \
                    from \
                        %s \
                    where \
                        not (array_contains(bundles,'com.next.innovation.takatak') or array_contains(bundles,'in.mohalla.video') or array_contains(bundles,'in.mohalla.sharechat')) \
                ) t2 \
                on \
                    t1.device_id = t2.device_id \
                where \
                    t1.device_id is not null \
                    and t1.target_app_label != 0 \
                    and t2.lastactiveday>='%s'  \
            " % (df_user_bundle_sample_date_after_table, df_user_bundle_sample_date_table,neg_sample_lastdate))
    return pos_data
def get_pos_data(spark,df_user_bundle_sample_date_table,df_user_bundle_sample_date_after_table,df_app_info_table):
    print("=" * 20)
    print("get_pos_data 开始进行正样本的初步构造")
    pos_data_ini = get_pos_data_ini(spark,df_user_bundle_sample_date_after_table,df_user_bundle_sample_date_table)
    pos_data_ini.createTempView("pos_data_temp_table")
    # 1.构建 bundle_social_set、bundle_social_set_size 特征
    pos_data_bundle_social_set = create_bundle_social_set("pos_data_temp_table",df_app_info_table)
    # 2.构建 activeday_label_1_cnt...activeday_label_30_cnt,activeday_label_1_01...activeday_label_30_01 特征
    pos_data_activeday_label_cnt = create_activeday_label_cnt(spark,"pos_data_temp_table")
    # 3.构建 bundles_size_label 特征
    pos_data_bundles_size_label = create_bundles_size_label(spark,"pos_data_temp_table")
    # 4.构建 lastdaygap_label 特征
    pos_data_lastdaygap_label = create_lastdaygap_label(spark,"pos_data_temp_table")
    # 组装
    pos_data = pos_data_ini.join(pos_data_bundle_social_set,"device_id",how="left") \
                           .join(pos_data_activeday_label_cnt,"device_id",how="left")   \
                           .join(pos_data_bundles_size_label,"device_id",how="left")   \
                           .join(pos_data_lastdaygap_label,"device_id",how="left")\
                           .select(
                                "device_id","osversion","target_app_label"
                                ,"bundle_social_set","bundle_social_set_size"
                                ,"activeday_label_1_cnt","activeday_label_2_cnt","activeday_label_3_cnt","activeday_label_4_cnt","activeday_label_5_cnt","activeday_label_6_cnt","activeday_label_7_cnt","activeday_label_8_cnt","activeday_label_9_cnt","activeday_label_10_cnt"
                                ,"activeday_label_1_01","activeday_label_2_01","activeday_label_3_01","activeday_label_4_01","activeday_label_5_01","activeday_label_6_01","activeday_label_7_01","activeday_label_8_01","activeday_label_9_01","activeday_label_10_01"
                                ,"activeday_label_11_cnt","activeday_label_12_cnt","activeday_label_13_cnt","activeday_label_14_cnt","activeday_label_15_cnt","activeday_label_16_cnt","activeday_label_17_cnt","activeday_label_18_cnt","activeday_label_19_cnt","activeday_label_20_cnt"
                                ,"activeday_label_11_01","activeday_label_12_01","activeday_label_13_01","activeday_label_14_01","activeday_label_15_01","activeday_label_16_01","activeday_label_17_01","activeday_label_18_01","activeday_label_19_01","activeday_label_20_01"
                                ,"activeday_label_21_cnt","activeday_label_22_cnt","activeday_label_23_cnt","activeday_label_24_cnt","activeday_label_25_cnt","activeday_label_26_cnt","activeday_label_27_cnt","activeday_label_28_cnt","activeday_label_29_cnt","activeday_label_30_cnt"
                                ,"activeday_label_21_01","activeday_label_22_01","activeday_label_23_01","activeday_label_24_01","activeday_label_25_01","activeday_label_26_01","activeday_label_27_01","activeday_label_28_01","activeday_label_29_01","activeday_label_30_01"
                                ,"bundles_size_label"
                                ,"lastdaygap_label"
                           )
    pos_data.repartition(50).write.mode("overwrite").orc(pos_sample_dirPath)
def get_neg_data_ini(spark,neg_sample_lastdate,pos_data_table,df_user_bundle_sample_date_table):
    neg_data_ini = spark.sql(" \
            select \
                t1.* \
            from \
            ( \
                select \
                    ifa as device_id \
                    ,bundles \
                    ,osversion \
                    ,lastactiveday \
                    ,activedays \
                from \
                    %s \
                where \
                    lastactiveday>='%s' \
                    and not (array_contains(bundles,'com.next.innovation.takatak') or array_contains(bundles,'in.mohalla.video') or array_contains(bundles,'in.mohalla.sharechat')) \
            ) t1 \
            left join \
                %s t2\
            on \
                t1.device_id = t2.device_id \
            where \
                t2.device_id is null \
        " % (df_user_bundle_sample_date_table, neg_sample_lastdate, pos_data_table))
    return neg_data_ini
def get_neg_data(spark,neg_sample_lastdate,pos_data_table,df_user_bundle_sample_date_table,df_app_info_table):
    print("=" * 20)
    print("get_neg_data 开始进行负样本的初步构造")
    neg_data_ini = get_neg_data_ini(spark,neg_sample_lastdate,pos_data_table,df_user_bundle_sample_date_table)
    neg_data_ini.createTempView("neg_data_temp_table")
    # 1.构建 bundle_social_set、bundle_social_set_size 特征
    neg_data_bundle_social_set = create_bundle_social_set("neg_data_temp_table", df_app_info_table)
    # 2.构建 activeday_label_1_cnt...activeday_label_30_cnt,activeday_label_1_01...activeday_label_30_01 特征
    neg_data_activeday_label_cnt = create_activeday_label_cnt(spark, "neg_data_temp_table")
    # 3.构建 bundles_size_label 特征
    neg_data_bundles_size_label = create_bundles_size_label(spark, "neg_data_temp_table")
    # 4.构建 lastdaygap_label 特征
    neg_data_lastdaygap_label = create_lastdaygap_label(spark, "neg_data_temp_table")
    # 组装
    neg_data = neg_data_ini.join(neg_data_bundle_social_set, "device_id",how="left") \
        .join(neg_data_activeday_label_cnt, "device_id", how="left") \
        .join(neg_data_bundles_size_label, "device_id", how="left") \
        .join(neg_data_lastdaygap_label, "device_id", how="left") \
        .select(
        "device_id", "osversion"
        , "bundle_social_set", "bundle_social_set_size"
        , "activeday_label_1_cnt", "activeday_label_2_cnt", "activeday_label_3_cnt", "activeday_label_4_cnt","activeday_label_5_cnt", "activeday_label_6_cnt", "activeday_label_7_cnt", "activeday_label_8_cnt","activeday_label_9_cnt", "activeday_label_10_cnt"
        , "activeday_label_1_01", "activeday_label_2_01", "activeday_label_3_01", "activeday_label_4_01","activeday_label_5_01", "activeday_label_6_01", "activeday_label_7_01", "activeday_label_8_01","activeday_label_9_01", "activeday_label_10_01"
        , "activeday_label_11_cnt", "activeday_label_12_cnt", "activeday_label_13_cnt", "activeday_label_14_cnt","activeday_label_15_cnt", "activeday_label_16_cnt", "activeday_label_17_cnt", "activeday_label_18_cnt","activeday_label_19_cnt", "activeday_label_20_cnt"
        , "activeday_label_11_01", "activeday_label_12_01", "activeday_label_13_01", "activeday_label_14_01","activeday_label_15_01", "activeday_label_16_01", "activeday_label_17_01", "activeday_label_18_01","activeday_label_19_01", "activeday_label_20_01"
        , "activeday_label_21_cnt", "activeday_label_22_cnt", "activeday_label_23_cnt", "activeday_label_24_cnt","activeday_label_25_cnt", "activeday_label_26_cnt", "activeday_label_27_cnt", "activeday_label_28_cnt","activeday_label_29_cnt", "activeday_label_30_cnt"
        , "activeday_label_21_01", "activeday_label_22_01", "activeday_label_23_01", "activeday_label_24_01","activeday_label_25_01", "activeday_label_26_01", "activeday_label_27_01", "activeday_label_28_01","activeday_label_29_01", "activeday_label_30_01"
        , "bundles_size_label"
        , "lastdaygap_label"
    )
    neg_data.repartition(200).write.mode("overwrite").orc(neg_sample_dirPath)

def get_neg_data_social(spark,neg_data_table,df_user_bundle_sample_date_after_table,df_app_info_table):
    print("=" * 20)
    print("get_neg_data_social 从负样本中，抽取第二天安装Social app的数据")

    neg_data_social = spark.sql(" \
        select \
            device_id \
            ,collect_set(bundle) as bundle_social_set_2 \
            ,size(collect_set(bundle)) as  bundle_social_set_2_size \
        from \
        ( \
            select \
                device_id \
                ,bundle \
            from \
            ( \
                select \
                    device_id \
                    ,bundle \
                from \
                ( \
                    select \
                        t_temp_1.ifa as device_id \
                        ,t_temp_1.bundles \
                    from \
                        %s t_temp_1\
                    inner join \
                        %s t_temp_2\
                    on \
                        t_temp_1.ifa = t_temp_2.device_id \
                )  t1 \
                lateral view \
                    explode(bundles) as bundle \
            ) t2 \
            inner join \
                %s t_temp_3\
            on \
                t2.bundle = t_temp_3.id \
            where \
                subcategory_name = 'Social' \
        ) t3 \
        group by \
            device_id"%(df_user_bundle_sample_date_after_table,neg_data_table,df_app_info_table))
    neg_data_social.repartition(50).write.mode("overwrite").orc(neg_sample_social_dirPath)
def get_ana_target_app_label():
    print("针对正样本的target_app_label进行分析")
    spark.sql(
        """
        select 
            target_app_label
            ,count(device_id) as device_id_cnt
        from 
            pos_data_table
        group by 
            target_app_label
        order by 
            device_id_cnt desc
        """
    ).show(100,truncate=False)
def get_ana_bundle_social_set(table_name):
    print("针对%s的bundle_social_set进行分析"%table_name)
    spark.sql(" \
        select  \
            bundle_social   \
            ,count(device_id) as device_id_cnt  \
        from    \
        (   \
            select  \
                device_id   \
                ,bundle_social  \
            from    \
                %s  \
            lateral view    \
                explode(bundle_social_set) as bundle_social \
        ) t1    \
        group by    \
            bundle_social   \
        order by    \
            device_id_cnt desc  \
    "%table_name).show(100,truncate=False)

def get_ana_bundle_social_set_size(table_name):
    print("针对%s的bundle_social_set_size进行分析"%table_name)
    spark.sql("select bundle_social_set_size,count(device_id) as device_id_cnt from %s group by bundle_social_set_size order by device_id_cnt desc"%table_name).show(100,truncate=False)

def get_ana_activeday_label_xx_cnt(table_name):
    print("针对%s的activeday_label_xx_cnt进行分析"%table_name)
    spark.sql(" \
        select  \
            avg(activeday_label_1_cnt) as avg_activeday_label_1_cnt    \
            ,avg(activeday_label_2_cnt) as avg_activeday_label_2_cnt    \
            ,avg(activeday_label_3_cnt) as avg_activeday_label_3_cnt    \
            ,avg(activeday_label_4_cnt) as avg_activeday_label_4_cnt    \
            ,avg(activeday_label_5_cnt) as avg_activeday_label_5_cnt    \
            ,avg(activeday_label_6_cnt) as avg_activeday_label_6_cnt    \
            ,avg(activeday_label_7_cnt) as avg_activeday_label_7_cnt    \
            ,avg(activeday_label_8_cnt) as avg_activeday_label_8_cnt    \
            ,avg(activeday_label_9_cnt) as avg_activeday_label_9_cnt    \
            ,avg(activeday_label_10_cnt) as avg_activeday_label_10_cnt  \
            ,avg(activeday_label_11_cnt) as avg_activeday_label_11_cnt  \
            ,avg(activeday_label_12_cnt) as avg_activeday_label_12_cnt  \
            ,avg(activeday_label_13_cnt) as avg_activeday_label_13_cnt  \
            ,avg(activeday_label_14_cnt) as avg_activeday_label_14_cnt  \
            ,avg(activeday_label_15_cnt) as avg_activeday_label_15_cnt  \
            ,avg(activeday_label_16_cnt) as avg_activeday_label_16_cnt  \
            ,avg(activeday_label_17_cnt) as avg_activeday_label_17_cnt  \
            ,avg(activeday_label_18_cnt) as avg_activeday_label_18_cnt  \
            ,avg(activeday_label_19_cnt) as avg_activeday_label_19_cnt  \
            ,avg(activeday_label_20_cnt) as avg_activeday_label_20_cnt  \
            ,avg(activeday_label_21_cnt) as avg_activeday_label_21_cnt  \
            ,avg(activeday_label_22_cnt) as avg_activeday_label_22_cnt  \
            ,avg(activeday_label_23_cnt) as avg_activeday_label_23_cnt  \
            ,avg(activeday_label_24_cnt) as avg_activeday_label_24_cnt  \
            ,avg(activeday_label_25_cnt) as avg_activeday_label_25_cnt  \
            ,avg(activeday_label_26_cnt) as avg_activeday_label_26_cnt  \
            ,avg(activeday_label_27_cnt) as avg_activeday_label_27_cnt  \
            ,avg(activeday_label_28_cnt) as avg_activeday_label_28_cnt  \
            ,avg(activeday_label_29_cnt) as avg_activeday_label_29_cnt  \
            ,avg(activeday_label_30_cnt) as avg_activeday_label_30_cnt  \
        from    \
            %s  \
    "%table_name).show(100,truncate=False)
def get_ana_activeday_label_xx_01(table_name):
    print("针对%s的activeday_label_xx_01进行分析"%table_name)
    spark.sql(" \
        select  \
            sum(activeday_label_1_01) as sum_activeday_label_1_01    \
            ,sum(activeday_label_2_01) as sum_activeday_label_2_01    \
            ,sum(activeday_label_3_01) as sum_activeday_label_3_01    \
            ,sum(activeday_label_4_01) as sum_activeday_label_4_01    \
            ,sum(activeday_label_5_01) as sum_activeday_label_5_01    \
            ,sum(activeday_label_6_01) as sum_activeday_label_6_01    \
            ,sum(activeday_label_7_01) as sum_activeday_label_7_01    \
            ,sum(activeday_label_8_01) as sum_activeday_label_8_01    \
            ,sum(activeday_label_9_01) as sum_activeday_label_9_01    \
            ,sum(activeday_label_10_01) as sum_activeday_label_10_01  \
            ,sum(activeday_label_11_01) as sum_activeday_label_11_01  \
            ,sum(activeday_label_12_01) as sum_activeday_label_12_01  \
            ,sum(activeday_label_13_01) as sum_activeday_label_13_01  \
            ,sum(activeday_label_14_01) as sum_activeday_label_14_01  \
            ,sum(activeday_label_15_01) as sum_activeday_label_15_01  \
            ,sum(activeday_label_16_01) as sum_activeday_label_16_01  \
            ,sum(activeday_label_17_01) as sum_activeday_label_17_01  \
            ,sum(activeday_label_18_01) as sum_activeday_label_18_01  \
            ,sum(activeday_label_19_01) as sum_activeday_label_19_01  \
            ,sum(activeday_label_20_01) as sum_activeday_label_20_01  \
            ,sum(activeday_label_21_01) as sum_activeday_label_21_01  \
            ,sum(activeday_label_22_01) as sum_activeday_label_22_01  \
            ,sum(activeday_label_23_01) as sum_activeday_label_23_01  \
            ,sum(activeday_label_24_01) as sum_activeday_label_24_01  \
            ,sum(activeday_label_25_01) as sum_activeday_label_25_01  \
            ,sum(activeday_label_26_01) as sum_activeday_label_26_01  \
            ,sum(activeday_label_27_01) as sum_activeday_label_27_01  \
            ,sum(activeday_label_28_01) as sum_activeday_label_28_01  \
            ,sum(activeday_label_29_01) as sum_activeday_label_29_01  \
            ,sum(activeday_label_30_01) as sum_activeday_label_30_01  \
        from    \
            %s  \
    "%table_name).show(100,truncate=False)
def get_ana_bundles_size_label(table_name):
    print("针对%s的bundles_size_label进行分析"%table_name)
    spark.sql("select bundles_size_label,count(device_id) as device_id_cnt from %s group by bundles_size_label order by device_id_cnt desc"%table_name).show(100,truncate=False)
def get_ana_lastdaygap_label(table_name):
    print("针对%s的lastdaygap_label进行分析"%table_name)
    spark.sql("select lastdaygap_label,count(device_id) as device_id_cnt from %s group by lastdaygap_label order by device_id_cnt desc" % table_name).show(100, truncate=False)
def get_ana_osversion(table_name):
    print("针对%s的osversion进行分析"%table_name)
    spark.sql("select osversion,count(device_id) as device_id_cnt from %s group by osversion order by device_id_cnt desc" % table_name).show(100, truncate=False)


# def get_pos_delay_ana(pos_data_table_ini,df_app_info_table):
#     # 考虑到数据延迟 例如 样本20220615 则分别统计20220616 20220617的转化
#     df_user_bundle_sample_date_middle = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=%s/platform=android/geo=IND" % (sample_date_middle))
#     df_user_bundle_sample_date_middle.createTempView("df_user_bundle_sample_date_middle_table")
#     print("pos_data_table_ini 用户 在%s进行Social app安装的数据分析"%sample_date_middle)
#     spark.sql(" \
#             select  \
#                 bundle  \
#                 ,count(device_id) as device_id_cnt  \
#             from    \
#             (   \
#                 select  \
#                     device_id   \
#                     ,bundle \
#                 from    \
#                 (   \
#                     select  \
#                         device_id   \
#                         ,bundle \
#                     from    \
#                     (   \
#                         select  \
#                             t1.ifa as device_id \
#                             ,t1.bundles \
#                         from    \
#                             %s t1   \
#                         inner join  \
#                             %s t2   \
#                         on  \
#                             t1.ifa =  t2.device_id  \
#                     ) t3    \
#                     lateral view    \
#                         explode(bundles) as bundle  \
#                 ) t4    \
#                 inner join  \
#                     %s t5   \
#                 on  \
#                     t4.bundle = t5.id   \
#                 where   \
#                     subcategory_name='Social'   \
#             ) t6    \
#             group by    \
#                 bundle  \
#             order by    \
#                 device_id_cnt desc  \
#         "%("df_user_bundle_sample_date_middle_table",pos_data_table_ini,df_app_info_table)).show(100,truncate=False)

def get_ana():
    # 正样本分析
    print("正样本分析")
    pos_data = spark.read.format('orc').load(pos_sample_dirPath_2)
    pos_data.createTempView("pos_data_table")
    get_ana_target_app_label()
    #
    get_ana_bundle_social_set("pos_data_table")
    get_ana_bundle_social_set_size("pos_data_table")
    get_ana_activeday_label_xx_cnt("pos_data_table")
    get_ana_activeday_label_xx_01("pos_data_table")
    get_ana_bundles_size_label("pos_data_table")
    get_ana_lastdaygap_label("pos_data_table")
    get_ana_osversion("pos_data_table")
    # 负样本分析
    print("负样本分析")
    neg_data = spark.read.format('orc').load(neg_sample_dirPath_2)
    neg_data.createTempView("neg_data_table")
    #
    get_ana_bundle_social_set("neg_data_table")
    get_ana_bundle_social_set_size("neg_data_table")
    get_ana_activeday_label_xx_cnt("neg_data_table")
    get_ana_activeday_label_xx_01("neg_data_table")
    get_ana_bundles_size_label("neg_data_table")
    get_ana_lastdaygap_label("neg_data_table")
    get_ana_osversion("neg_data_table")
    # # 负social样本分析
    # print("负social样本分析")
    # neg_data_social = spark.read.format('orc').load(neg_sample_social_dirPath_2)
    # neg_data_social.createTempView("neg_data_social_table")
    # #
    # get_ana_bundle_social_set("neg_data_social_table")
    # get_ana_bundle_social_set_size("neg_data_social_table")
    # get_ana_activeday_label_xx_cnt("neg_data_social_table")
    # get_ana_activeday_label_xx_01("neg_data_social_table")
    # get_ana_bundles_size_label("neg_data_social_table")
    # get_ana_lastdaygap_label("neg_data_social_table")
    # get_ana_osversion("neg_data_social_table")
    # print("针对neg_data_social_table，统计在第二天进行安装Social app的个数")
    # spark.sql("select bundle_social_set_2_size,count(device_id) as device_id_cnt from %s group by bundle_social_set_2_size order by device_id_cnt desc"%"neg_data_social_table").show(100, truncate=False)
    # print("针对neg_data_social_table，统计在第二天进行安装Social app的种类 以及用户数")
    # spark.sql(" \
    #         select  \
    #             bundle_social_set_2_ele \
    #             ,count(distinct device_id) as device_id_cnt \
    #         from    \
    #         (   \
    #             select  \
    #                 device_id   \
    #                 ,bundle_social_set_2_ele    \
    #             from    \
    #                 %s   \
    #             lateral view    \
    #                 explode(bundle_social_set_2) as bundle_social_set_2_ele \
    #         ) t1    \
    #         group by    \
    #             bundle_social_set_2_ele \
    #         order by    \
    #             device_id_cnt desc  \
    #     " % "neg_data_social_table").show(100, truncate=False)


def get_sample():
    pass
def main(spark,sample_date,sample_date_after,neg_sample_lastdate):

    df_user_bundle_sample_date = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=%s/platform=android/geo=IND" % (sample_date))
    df_user_bundle_sample_date.createTempView("df_user_bundle_sample_date_table")
    df_user_bundle_sample_date_after = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=%s/platform=android/geo=IND" % (sample_date_after))
    df_user_bundle_sample_date_after.createTempView("df_user_bundle_sample_date_after_table")

    # 加载bundle 信息
    df_app_info = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_app_info/")
    df_app_info.createTempView("df_app_info_table")
    # 缺失值填充
    bundle_social_set_default = F.array(F.lit("N"))
    fill_rule = F.when(F.col("bundle_social_set").isNull(), bundle_social_set_default).otherwise(F.col("bundle_social_set"))

    # 正样本
    get_pos_data(spark, "df_user_bundle_sample_date_table", "df_user_bundle_sample_date_after_table","df_app_info_table")
    pos_data = spark.read.format('orc').load(pos_sample_dirPath)
    pos_data_fill = pos_data.fillna("N",subset=["osversion"]).fillna(0, subset=["bundle_social_set_size"]).withColumn("bundle_social_set_temp", fill_rule).drop("bundle_social_set").withColumnRenamed("bundle_social_set_temp","bundle_social_set")
    pos_data_fill.repartition(50).write.mode("overwrite").orc(pos_sample_dirPath_2)
    pos_data_fill.createTempView("pos_data_table_ini")



    # 负样本
    get_neg_data(spark,neg_sample_lastdate, "pos_data_table_ini","df_user_bundle_sample_date_after_table","df_app_info_table")
    neg_data = spark.read.format("orc").load(neg_sample_dirPath)
    neg_data_fill = neg_data.fillna("N",subset=["osversion"]).fillna(0, subset=["bundle_social_set_size"]).withColumn("bundle_social_set_temp", fill_rule).drop("bundle_social_set").withColumnRenamed("bundle_social_set_temp","bundle_social_set")
    neg_data_fill.repartition(200).write.mode("overwrite").orc(neg_sample_dirPath_2)

    # neg_data_fill.createTempView("neg_data_table_ini")

    # # 针对负样本 抽取第二天下载social app 的数据
    # get_neg_data_social(spark, "neg_data_table_ini", "df_user_bundle_sample_date_after_table","df_app_info_table")
    # neg_data_social = spark.read.format("orc").load(neg_sample_social_dirPath)
    # fill_rule_social = F.when(F.col("bundle_social_set_2").isNull(), bundle_social_set_default).otherwise(F.col("bundle_social_set_2"))
    # neg_data_social_fill = neg_data_social.fillna("N",subset=["osversion"]).fillna(0, subset=["bundle_social_set_2_size"]).withColumn("bundle_social_set_2_temp", fill_rule_social).drop("bundle_social_set_2").withColumnRenamed("bundle_social_set_2_temp","bundle_social_set_2")
    # neg_data_social_fill.createTempView("neg_data_social_fill_ini")
    # spark.sql("""
    #     select
    #         t1.*
    #         ,t2.bundle_social_set_2_size
    #         ,t2.bundle_social_set_2
    #     from
    #         neg_data_table_ini t1
    #     inner join
    #         neg_data_social_fill_ini t2
    #     on
    #         t1.device_id = t2.device_id
    #     where
    #         t1.bundle_social_set_size < t2.bundle_social_set_2_size
    # """).repartition(50).write.mode("overwrite").orc(neg_sample_social_dirPath_2)

    # 正样本、负样本、负scoial样本数据分析
    get_ana()
    # 正样本、负样本组装
    get_sample()
def save_output_to_oss(spark):
    sc=spark.sparkContext
    applicationId=str(sc.applicationId)
    print(f"driver applicationId {applicationId}")
    command_1=f'echo {applicationId} > driver_applicationId.log'
    os.system(command_1)
    command_2=f"hdfs dfs -put -f driver_applicationId.log  oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/"
    os.system(command_2)

if __name__ == '__main__':
    print("logs_start")
    spark = SparkSession.builder.appName("sample").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample_date', help='基于用户安装列表的样本构造日期')
    parser.add_argument('--lastActivedays', help='用户最后一天活跃',default=3)
    args = parser.parse_args()
    print("传入参数 args",args)
    sample_date = args.sample_date
    sample_date_date = datetime.datetime.strptime(sample_date, "%Y%m%d")
    print('样本构造日期 sample_date_date',sample_date_date)
    lastActivedays = int(args.lastActivedays)
    print('最近活跃天数 lastActivedays',lastActivedays)


    # 考虑到特征的延迟 例如20220615 的数据，则20220616 19点左右才能落到oss。则20220615的数据只能用到20220617的投放
    days_delay = 2
    sample_suffix = "_delay_%d" % lastActivedays
    sample_date_after = (sample_date_date + datetime.timedelta(days=days_delay)).strftime("%Y%m%d")
    print('样本构造参考日期 sample_date_after',sample_date_after)


    now_date = datetime.datetime.now().strftime("%Y%m%d")
    print('当前日期 now_date',now_date)

    neg_sample_lastdate = (sample_date_date - datetime.timedelta(days=lastActivedays)).strftime("%Y%m%d") # 当为7 时 数据量2亿，太大。计算social时内存溢出
    print('负样本筛选参考日期 neg_sample_lastdate',neg_sample_lastdate)

    oss_dirPath = 'oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app'
    pos_sample_dirPath = "%s/oss_%s_pos_sample%s"%(oss_dirPath,sample_date,sample_suffix)
    pos_sample_dirPath_2 = "%s/oss_%s_pos_sample%s_2"%(oss_dirPath,sample_date,sample_suffix)
    neg_sample_dirPath = "%s/oss_%s_neg_sample%s"%(oss_dirPath,sample_date,sample_suffix)
    neg_sample_dirPath_2 = "%s/oss_%s_neg_sample%s_2"%(oss_dirPath,sample_date,sample_suffix)
    neg_sample_social_dirPath = "%s/oss_%s_neg_sample_social%s" % (oss_dirPath, sample_date,sample_suffix)
    neg_sample_social_dirPath_2 = "%s/oss_%s_neg_sample_social%s_2" % (oss_dirPath, sample_date,sample_suffix)

    # 构造样本
    if now_date < sample_date_after:
        print("当前日期<样本构造参考日期,因此无法构造样本")
        sys.exit()
    main(spark,sample_date,sample_date_after,neg_sample_lastdate)
    print("logs_end")
    save_output_to_oss(spark)










