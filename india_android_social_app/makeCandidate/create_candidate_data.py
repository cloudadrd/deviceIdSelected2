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
            ,if(size(bundles)>=50,50,size(bundles)) as bundles_size_label \
        from    \
            %s  \
    "%pos_data_temp_table)
    return pos_data_bundles_size_label
def create_lastdaygap_label(spark,pos_data_temp_table):
    print("对表%s构建 lastdaygap_label特征"%pos_data_temp_table)
    pos_data_lastdaygap_label = spark.sql("  \
        select  \
            device_id   \
            ,if(activedays[size(activedays)-1]>=50,50,activedays[size(activedays)-1]) as  lastdaygap_label   \
        from    \
            %s  \
    "%pos_data_temp_table)
    return pos_data_lastdaygap_label


def get_candidate_data_ini(spark,neg_sample_lastdate,df_user_bundle_sample_date_table):
    neg_data_ini = spark.sql(" \
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
        " % (df_user_bundle_sample_date_table, neg_sample_lastdate))
    return neg_data_ini


def get_candidate_data(spark , neg_sample_lastdate ,df_user_bundle_sample_date_table,df_app_info_table):
    print("=" * 20)
    print("get_candidate_data 开始进行人群库的初步构造")
    neg_data_ini = get_candidate_data_ini(spark, neg_sample_lastdate, df_user_bundle_sample_date_table)
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
    neg_data = neg_data_ini.join(neg_data_bundle_social_set, "device_id", how="left") \
        .join(neg_data_activeday_label_cnt, "device_id", how="left") \
        .join(neg_data_bundles_size_label, "device_id", how="left") \
        .join(neg_data_lastdaygap_label, "device_id", how="left") \
        .select(
        "device_id", "osversion"
        , "bundle_social_set", "bundle_social_set_size"
        , "activeday_label_1_cnt", "activeday_label_2_cnt", "activeday_label_3_cnt", "activeday_label_4_cnt",
        "activeday_label_5_cnt", "activeday_label_6_cnt", "activeday_label_7_cnt", "activeday_label_8_cnt",
        "activeday_label_9_cnt", "activeday_label_10_cnt"
        , "activeday_label_1_01", "activeday_label_2_01", "activeday_label_3_01", "activeday_label_4_01",
        "activeday_label_5_01", "activeday_label_6_01", "activeday_label_7_01", "activeday_label_8_01",
        "activeday_label_9_01", "activeday_label_10_01"
        , "activeday_label_11_cnt", "activeday_label_12_cnt", "activeday_label_13_cnt", "activeday_label_14_cnt",
        "activeday_label_15_cnt", "activeday_label_16_cnt", "activeday_label_17_cnt", "activeday_label_18_cnt",
        "activeday_label_19_cnt", "activeday_label_20_cnt"
        , "activeday_label_11_01", "activeday_label_12_01", "activeday_label_13_01", "activeday_label_14_01",
        "activeday_label_15_01", "activeday_label_16_01", "activeday_label_17_01", "activeday_label_18_01",
        "activeday_label_19_01", "activeday_label_20_01"
        , "activeday_label_21_cnt", "activeday_label_22_cnt", "activeday_label_23_cnt", "activeday_label_24_cnt",
        "activeday_label_25_cnt", "activeday_label_26_cnt", "activeday_label_27_cnt", "activeday_label_28_cnt",
        "activeday_label_29_cnt", "activeday_label_30_cnt"
        , "activeday_label_21_01", "activeday_label_22_01", "activeday_label_23_01", "activeday_label_24_01",
        "activeday_label_25_01", "activeday_label_26_01", "activeday_label_27_01", "activeday_label_28_01",
        "activeday_label_29_01", "activeday_label_30_01"
        , "bundles_size_label"
        , "lastdaygap_label"
    )
    neg_data.repartition(200).write.mode("overwrite").orc(candidate_data_dirPath)
def main(spark,sample_date,neg_sample_lastdate):

    df_user_bundle_sample_date = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=%s/platform=android/geo=IND" % (sample_date))
    df_user_bundle_sample_date.createTempView("df_user_bundle_sample_date_table")
    # 加载bundle 信息
    df_app_info = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_app_info/")
    df_app_info.createTempView("df_app_info_table")
    # 缺失值填充
    bundle_social_set_default = F.array(F.lit("N"))
    fill_rule = F.when(F.col("bundle_social_set").isNull(), bundle_social_set_default).otherwise(F.col("bundle_social_set"))

    # create_candidate_data
    get_candidate_data(spark,neg_sample_lastdate, "df_user_bundle_sample_date_table","df_app_info_table")

    candidate_data = spark.read.format('orc').load(candidate_data_dirPath)
    candidate_data_fill = candidate_data.fillna("N",subset=["osversion"]).fillna(0, subset=["bundle_social_set_size"]).withColumn("bundle_social_set_temp", fill_rule).drop("bundle_social_set").withColumnRenamed("bundle_social_set_temp","bundle_social_set").persist()
    candidate_data_fill.repartition(50).write.mode("overwrite").orc(candidate_data_dirPath_2)
    print("人群库的数量",candidate_data_fill.count())

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
    print('最近活跃天数 lastActivedays', lastActivedays)

    neg_sample_lastdate = (sample_date_date - datetime.timedelta(days=lastActivedays)).strftime("%Y%m%d") # 当为7 时 数据量2亿，太大。计算social时内存溢出
    print('人群库筛选参考日期 neg_sample_lastdate',neg_sample_lastdate)

    if lastActivedays == 3:
        candidate_suffix=""
    else:
        candidate_suffix = "_%d" % lastActivedays
    oss_dirPath = 'oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app'
    candidate_data_dirPath = "%s/oss_%s_candidate%s"%(oss_dirPath,sample_date,candidate_suffix)
    candidate_data_dirPath_2 = "%s/oss_%s_candidate%s_2"%(oss_dirPath,sample_date,candidate_suffix)


    main(spark,sample_date,neg_sample_lastdate)
    print("logs_end")
    save_output_to_oss(spark)









