# -*- coding: utf-8 -*-
import argparse
import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import datetime
import warnings
warnings.filterwarnings('ignore')
import os
def create_bundle_app_set(pos_data_temp_table,df_app_info_table):
    print("对表%s构建 bundle_app_set、bundle_app_set_size 特征"%pos_data_temp_table)
    pos_data_bundle_app_set = spark.sql(f"""
        select 
            device_id
            ,collect_set(bundle) as bundle_app_set
            ,size(collect_set(bundle)) as bundle_app_set_size
        from 
        (
            select 
                device_id
                ,bundle
            from 
            (
                select 
                    device_id
                    ,bundle
                from 
                    {pos_data_temp_table}
                lateral view
                    explode(bundles) as bundle
            ) t1 
            left join 
                {df_app_info_table} t_app_info_table
            on 
                t1.bundle = t_app_info_table.id
            where 
                subcategory_name='{pkg_subcategory}'
        )t2 
        group by 
            device_id
    """)
    return pos_data_bundle_app_set


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



def get_neg_pos_ini(spark,df_user_bundle_sample_date_table,df_user_bundle_sample_date_after_table):
    df_neg_pos_ini = spark.sql(f"""
        select 
            t2.device_id
            ,bundles
            ,osversion
            ,lastactiveday
            ,activedays
            ,if(t1.device_id is null,0,1) as after_label
            ,now_label
        from 
        (
        select 
            ifa as device_id
        from 
            {df_user_bundle_sample_date_after_table}
        where 
             {array_contains_sql}
        ) t1
        right join 
        (
            select 
                ifa as device_id
                ,bundles
                ,osversion
                ,lastactiveday
                ,activedays
                ,if({array_contains_sql},1,0) as now_label
            from 
                {df_user_bundle_sample_date_table}
        ) t2
        on 
            t1.device_id = t2.device_id 
    """)
    return df_neg_pos_ini
def get_app_delay(spark, df_user_bundle_sample_date_after_table,df_app_info_table):
    app_delay = spark.sql(f"""
        select 
            device_id
            ,size(collect_set(bundle)) as delay_bundle_app_set_size
            ,collect_set(bundle) as delay_bundle_app_set
        from 
        (
            select  
                device_id
                ,bundle
            from 
            (
                select 
                    ifa as device_id
                    ,bundle
                from 
                    {df_user_bundle_sample_date_after_table}
                lateral view
                    explode(bundles) as bundle
            ) t1
            inner join 
                {df_app_info_table} as app_info_table
            on 
                t1.bundle = app_info_table.id
            where 
                subcategory_name = '{pkg_subcategory}'
        ) t2
        group by 
            device_id 
    """)
    return app_delay
def main():
    df_user_bundle_sample_date = spark.read.format("orc").load(
        "oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=%s/platform=android/geo=IDN" % (sample_date))
    df_user_bundle_sample_date.createTempView("df_user_bundle_sample_date_table")
    df_user_bundle_sample_date_after = spark.read.format("orc").load(
        "oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=%s/platform=android/geo=IDN" % (
            sample_date_after))
    df_user_bundle_sample_date_after.createTempView("df_user_bundle_sample_date_after_table")
    # 加载bundle 信息
    df_app_info = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_app_info/")
    df_app_info.createTempView("df_app_info_table")
    # 创建样本
    df_neg_pos_ini=get_neg_pos_ini(spark, "df_user_bundle_sample_date_table", "df_user_bundle_sample_date_after_table")
    df_neg_pos_ini.createTempView("df_neg_pos_ini_table")
    # 1.构建 bundle_app_set、bundle_app_set_size 特征
    pos_data_bundle_app_set = create_bundle_app_set("df_neg_pos_ini_table", "df_app_info_table")
    # 2.构建 activeday_label_1_cnt...activeday_label_30_cnt,activeday_label_1_01...activeday_label_30_01 特征
    pos_data_activeday_label_cnt = create_activeday_label_cnt(spark, "df_neg_pos_ini_table")
    # 3.构建 bundles_size_label 特征
    pos_data_bundles_size_label = create_bundles_size_label(spark, "df_neg_pos_ini_table")
    # 4.构建 lastdaygap_label 特征
    pos_data_lastdaygap_label = create_lastdaygap_label(spark, "df_neg_pos_ini_table")
    # 组装
    pos_data = df_neg_pos_ini.join(pos_data_bundle_app_set,"device_id",how="left") \
                           .join(pos_data_activeday_label_cnt,"device_id",how="left")   \
                           .join(pos_data_bundles_size_label,"device_id",how="left")   \
                           .join(pos_data_lastdaygap_label,"device_id",how="left")\
                           .select(
                                "device_id","osversion","now_label","after_label"
                                ,"bundle_app_set","bundle_app_set_size"
                                ,"activeday_label_1_cnt","activeday_label_2_cnt","activeday_label_3_cnt","activeday_label_4_cnt","activeday_label_5_cnt","activeday_label_6_cnt","activeday_label_7_cnt","activeday_label_8_cnt","activeday_label_9_cnt","activeday_label_10_cnt"
                                ,"activeday_label_1_01","activeday_label_2_01","activeday_label_3_01","activeday_label_4_01","activeday_label_5_01","activeday_label_6_01","activeday_label_7_01","activeday_label_8_01","activeday_label_9_01","activeday_label_10_01"
                                ,"activeday_label_11_cnt","activeday_label_12_cnt","activeday_label_13_cnt","activeday_label_14_cnt","activeday_label_15_cnt","activeday_label_16_cnt","activeday_label_17_cnt","activeday_label_18_cnt","activeday_label_19_cnt","activeday_label_20_cnt"
                                ,"activeday_label_11_01","activeday_label_12_01","activeday_label_13_01","activeday_label_14_01","activeday_label_15_01","activeday_label_16_01","activeday_label_17_01","activeday_label_18_01","activeday_label_19_01","activeday_label_20_01"
                                ,"activeday_label_21_cnt","activeday_label_22_cnt","activeday_label_23_cnt","activeday_label_24_cnt","activeday_label_25_cnt","activeday_label_26_cnt","activeday_label_27_cnt","activeday_label_28_cnt","activeday_label_29_cnt","activeday_label_30_cnt"
                                ,"activeday_label_21_01","activeday_label_22_01","activeday_label_23_01","activeday_label_24_01","activeday_label_25_01","activeday_label_26_01","activeday_label_27_01","activeday_label_28_01","activeday_label_29_01","activeday_label_30_01"
                                ,"bundles_size_label"
                                ,"lastdaygap_label"
                           )
    # 缺失值填充
    bundle_app_set_default = F.array(F.lit("N"))
    fill_rule = F.when(F.col("bundle_app_set").isNull(), bundle_app_set_default).otherwise(F.col("bundle_app_set"))
    pos_data_fill = pos_data.fillna("N",subset=["osversion"]).fillna(0, subset=["bundle_app_set_size"]).withColumn("bundle_app_set_temp", fill_rule).drop("bundle_app_set").withColumnRenamed("bundle_app_set_temp","bundle_app_set")
    pos_data_fill.repartition(200).write.mode("overwrite").orc(sample_ana_dirPath)

    # 5.构建app 类型有需求的所有用户
    app_delay = get_app_delay(spark, "df_user_bundle_sample_date_after_table","df_app_info_table")
    delay_fill_rule = F.when(F.col("delay_bundle_app_set").isNull(), bundle_app_set_default).otherwise(F.col("delay_bundle_app_set"))
    delay_pos_data_fill = app_delay.fillna(0, subset=["delay_bundle_app_set_size"]).withColumn("delay_bundle_app_set_temp", delay_fill_rule).drop("delay_bundle_app_set").withColumnRenamed("delay_bundle_app_set_temp","delay_bundle_app_set")
    delay_pos_data_fill.repartition(200).write.mode("overwrite").orc(sample_ana_delay_dirPath)

def get_pkg_sql_info():
    """
    根据包名构建相关sql
    :return:
    """
    pkg_list = pkg_name.split("&&||&&")

    array_contains_sql = ""
    target_app_label_sql = " case "
    """
    (1) array_contains(bundles, "com.lazada.android")
    (2)
    case \
    when array_contains(bundles,'com.next.innovation.takatak') then 1 \
    when array_contains(bundles,'in.mohalla.video') then 2 \
    when array_contains(bundles,'in.mohalla.sharechat') then 3 \
    else 0 \
    end as target_app_label \
    """
    for i in range(len(pkg_list)):
        pkg_temp = pkg_list[i]
        sql_temp_1 = f" array_contains(bundles,'{pkg_temp}') "
        sql_temp_2 = f" when array_contains(bundles,'{pkg_temp}') then '{pkg_temp}' "
        if i == len(pkg_list)-1:
            array_contains_sql += sql_temp_1
        else:
            array_contains_sql += sql_temp_1 + " or "
        target_app_label_sql += sql_temp_2
    target_app_label_sql += " else '0' end as target_app_label"
    print(array_contains_sql)
    return array_contains_sql,target_app_label_sql






def get_pkg_info():
    # 加载bundle 信息
    df_app_info = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_app_info/")
    df_app_info.createTempView("df_app_info_table")
    spark.sql(
        """
        select 
            *
        from 
            df_app_info_table
        where 
            df_app_info_table.id = "com.lazada.android"
        """
    ).show()
    #**********************
    #+------------------+--------------+------------+-------------+--------------------+--------------------+----------------+--------------+--------------------+-----------+-----------------------+--------------------+--------------------+------------+----------------+------------------------+----------------+-----------------+--------------------+--------------------+--------------------+--------------+-----------+---------------+--------------------+-----------------+----------------------+--------------------+--------------------+--------------------+--------------------+------------------+-----------------+---------+---------+----+----------+-------+
    # |                id|compatible_ios|publisher_id|category_name|        other_stores|         description|subcategory_name|subcategory_id|        apptopia_url|category_id|offers_in_app_purchases|         permissions|         privacy_url|category_ids|age_restrictions|cross_store_publisher_id|last_update_date|approx_size_bytes|     screenshot_urls|            icon_url|initial_release_date|publisher_name|price_cents|current_version|               icons|operating_systems|vnd_estimated_installs|       publisher_url|            subtitle|                name|       app_store_url|cross_store_app_id|parent_company_id|languages|bundle_id|tags|update_day|   plat|
    # +------------------+--------------+------------+-------------+--------------------+--------------------+----------------+--------------+--------------------+-----------+-----------------------+--------------------+--------------------+------------+----------------+------------------------+----------------+-----------------+--------------------+--------------------+--------------------+--------------+-----------+---------------+--------------------+-----------------+----------------------+--------------------+--------------------+--------------------+--------------------+------------------+-----------------+---------+---------+----+----------+-------+
    # |com.lazada.android|          null|       53062| Applications|[[store -> itunes...|Get the most out ...|        Shopping|            26|https://apptopia....|         39|                  false|[retrieve running...|https://www.lazad...|    [39, 26]|        Everyone|              8090112233|      2022-01-14|         58720256|[https://play-lh....|https://play-lh.g...|          2014-06-01| Lazada Mobile|       null|         6.92.1|[128x128 -> https...|       4.4 and up|          100,000,000+|http://www.lazada...|Shop Everything Y...|Lazada - Grant De...|https://play.goog...|        8119537414|       8080232140|     null|     null|null|  20220509|android|
    # +------------------+--------------+------------+-------------+--------------------+--------------------+----------------+--------------+--------------------+-----------+-----------------------+--------------------+--------------------+------------+----------------+------------------------+----------------+-----------------+--------------------+--------------------+--------------------+--------------+-----------+---------------+--------------------+-----------------+----------------------+--------------------+--------------------+--------------------+--------------------+------------------+-----------------+---------+---------+----+----------+-------+
    #**********************

if __name__ == '__main__':
    print("logs_start")
    spark = SparkSession.builder.appName("lazada").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    parser = argparse.ArgumentParser()
    parser.add_argument('--sample_date', help='基于用户安装列表的样本构造日期')
    parser.add_argument('--days_delay', help='构建正样本的参考天数', default=1)
    parser.add_argument('--geo', help='国家')
    parser.add_argument('--platform', help='平台')
    parser.add_argument('--oss_dirname', help='oss根目录名称')
    parser.add_argument('--pkg_name',help='目标包名称,若为多个包则以&&||&&进行分割')
    parser.add_argument('--pkg_subcategory',help='目标包类别')
    args = parser.parse_args()
    print("传入参数 args", args)

    sample_date = args.sample_date
    days_delay = int(args.days_delay)
    platform = args.platform
    geo= args.geo
    oss_dirname= args.oss_dirname
    pkg_name= args.pkg_name
    pkg_subcategory= args.pkg_subcategory



    sample_date_date = datetime.datetime.strptime(sample_date, "%Y%m%d")
    sample_date_after = (sample_date_date + datetime.timedelta(days=days_delay)).strftime("%Y%m%d")
    print("sample_date_after",sample_date_after)
    oss_path = f"oss://sdkemr-yeahmobi/user/chensheng/pkg/{oss_dirname}/{geo}_{platform}"
    sample_ana_dirPath = f"{oss_path}/sample_ana/{sample_date}_delay_{days_delay}"
    sample_ana_delay_dirPath = f"{oss_path}/sample_ana/{sample_date}_delay_{days_delay}_delay"
    print("sample_ana_dirPath",sample_ana_dirPath)
    array_contains_sql, target_app_label_sql = get_pkg_sql_info()
    main()

    print("logs_end")