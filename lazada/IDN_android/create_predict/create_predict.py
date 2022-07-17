# -*- coding: utf-8 -*-
import argparse
import sys
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType,IntegerType,StringType
from pyspark.sql import SparkSession
import datetime
import warnings
warnings.filterwarnings('ignore')
import os


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



def get_predict_ini(spark,df_user_bundle_sample_date_table,df_app_info_table):
    now_info = spark.sql(f"""
        select
            device_id
            ,if({array_contains_sql},1,0) as now_label
            ,bundles as now_app_set
            ,if(size(bundles) is null,0,size(bundles)) as now_app_set_size
            ,{target_app_label_sql} as now_target_app_label
        from
        (
            select
                device_id
                ,collect_list(bundle) as bundles
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
                        {df_user_bundle_sample_date_table}
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
        ) t3
    """
    )
    now_info.createTempView("now_info_table")
    data_ana = spark.sql("""
        select
            now_table.ifa as device_id
            ,osversion
            ,lastactiveday
            ,activedays
            ,if(activedays[size(activedays)-1]>=499,499,activedays[size(activedays)-1]) as  lastdaygap_label
            ,if(size(bundles)>=499,499,size(bundles)) as bundles_size_label
            ,if(now_label is null,0,now_label) as now_label
            ,now_app_set
            ,if(now_app_set_size is null,0,now_app_set_size) as now_app_set_size
            ,if(now_target_app_label is null,"0",now_target_app_label) as now_target_app_label
        from 
            df_user_bundle_sample_date_table as now_table
        left join 
            now_info_table
        on 
            now_info_table.device_id = now_table.ifa
    """)
    return data_ana


def main():
    df_user_bundle_sample_date = spark.read.format("orc").load(f"oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day={sample_date}/platform={platform}/geo={geo}").where(f"lastactiveday>={lastactiveday_end}")
    df_user_bundle_sample_date.createTempView("df_user_bundle_sample_date_table")
    # 加载bundle 信息
    df_app_info = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_app_info/")
    df_app_info.createTempView("df_app_info_table")
    # 创建分析所用数据
    data_predict = get_predict_ini(spark,"df_user_bundle_sample_date_table","df_app_info_table")
    data_predict.createTempView("data_predict_table")
    # 构建 activeday_label_1_cnt...activeday_label_30_cnt,activeday_label_1_01...activeday_label_30_01 特征
    data_predict_1 = create_activeday_label_cnt(spark, "data_predict_table")

    # 组装
    data_ana_all = data_predict.join(data_predict_1,"device_id",how="left")
    # 缺失值填充
    bundle_social_set_default = F.array(F.lit("N"))
    # after_app_set now_app_set
    now_fill_rule = F.when(F.col("now_app_set").isNull(), bundle_social_set_default).otherwise(F.col("now_app_set"))
    data_ana_all_fill = data_ana_all.fillna("N", subset=["osversion"]).fillna(0,subset=["now_app_set_size"]) \
                                    .withColumn("now_app_set_temp", now_fill_rule).drop("now_app_set").withColumnRenamed("now_app_set_temp","now_app_set")
    # 保存
    # 构建 样本
    data_ana_all_fill.createTempView("data_ana_all_fill_table")
    predict_sample = spark.sql(f"""
           select
               device_id, osversion
               , activeday_label_1_cnt, activeday_label_2_cnt, activeday_label_3_cnt, activeday_label_4_cnt, activeday_label_5_cnt, activeday_label_6_cnt, activeday_label_7_cnt, activeday_label_8_cnt, activeday_label_9_cnt, activeday_label_10_cnt
               , activeday_label_1_01, activeday_label_2_01, activeday_label_3_01, activeday_label_4_01, activeday_label_5_01, activeday_label_6_01, activeday_label_7_01, activeday_label_8_01, activeday_label_9_01, activeday_label_10_01
               , activeday_label_11_cnt, activeday_label_12_cnt, activeday_label_13_cnt, activeday_label_14_cnt, activeday_label_15_cnt, activeday_label_16_cnt, activeday_label_17_cnt, activeday_label_18_cnt, activeday_label_19_cnt, activeday_label_20_cnt
               , activeday_label_11_01, activeday_label_12_01, activeday_label_13_01, activeday_label_14_01, activeday_label_15_01, activeday_label_16_01, activeday_label_17_01, activeday_label_18_01, activeday_label_19_01, activeday_label_20_01
               , activeday_label_21_cnt, activeday_label_22_cnt, activeday_label_23_cnt, activeday_label_24_cnt, activeday_label_25_cnt, activeday_label_26_cnt, activeday_label_27_cnt, activeday_label_28_cnt, activeday_label_29_cnt, activeday_label_30_cnt
               , activeday_label_21_01, activeday_label_22_01, activeday_label_23_01, activeday_label_24_01, activeday_label_25_01, activeday_label_26_01, activeday_label_27_01, activeday_label_28_01, activeday_label_29_01, activeday_label_30_01
               , bundles_size_label
               , lastdaygap_label
           from
               data_ana_all_fill_table
           where
               lastdaygap_label <= {lastdaygap}
               and now_app_set_size = 0
               and now_label = 0
       """)
    predict_sample.repartition(200).write.mode("overwrite").orc(predict_dirPath)


def get_pkg_sql_info(bundles):
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
        sql_temp_1 = f" array_contains({bundles},'{pkg_temp}') "
        sql_temp_2 = f" when array_contains({bundles},'{pkg_temp}') then '{pkg_temp}' "
        if i == len(pkg_list)-1:
            array_contains_sql += sql_temp_1
        else:
            array_contains_sql += sql_temp_1 + " or "
        target_app_label_sql += sql_temp_2
    target_app_label_sql += " else '0' end "
    print(array_contains_sql)
    return array_contains_sql,target_app_label_sql


def save_output_to_oss(spark):
    sc=spark.sparkContext
    applicationId=str(sc.applicationId)
    print(f"driver applicationId {applicationId}")
    command_1=f'echo {applicationId} > driver_applicationId.log'
    os.system(command_1)
    command_2=f"hdfs dfs -put -f driver_applicationId.log  {oss_dirPath}/predict/logs/"
    os.system(command_2)

if __name__ == '__main__':
    print("logs_start")
    spark = SparkSession.builder.appName("lazada").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    parser = argparse.ArgumentParser()
    parser.add_argument('--sample_date', help='基于用户安装列表的样本构造日期')
    parser.add_argument('--geo', help='国家')
    parser.add_argument('--platform', help='平台')
    parser.add_argument('--oss_dirname', help='oss根目录名称')
    parser.add_argument('--pkg_name',help='目标包名称,若为多个包则以&&||&&进行分割')
    parser.add_argument('--pkg_subcategory',help='目标包类别')
    parser.add_argument('--lastdaygap',help='最后活跃距今时间')
    args = parser.parse_args()
    print("传入参数 args", args)

    sample_date = args.sample_date
    platform = args.platform
    geo= args.geo
    oss_dirname= args.oss_dirname
    pkg_name= args.pkg_name
    pkg_subcategory= args.pkg_subcategory
    lastdaygap= int(args.lastdaygap)

    sample_date_date = datetime.datetime.strptime(sample_date, "%Y%m%d")
    # 由于用户存在过期现象,选择近30天活跃的用户
    lastactiveday_end = (sample_date_date - datetime.timedelta(days=30)).strftime("%Y%m%d")
    print("lastactiveday_end",lastactiveday_end)
    #
    oss_dirPath=f"oss://sdkemr-yeahmobi/user/chensheng/pkg/{oss_dirname}/{geo}_{platform}"
    predict_dirPath = f"{oss_dirPath}/predict/{sample_date}_lastdaygap_{lastdaygap}"
    print("predict_dirPath",predict_dirPath)
    array_contains_sql, target_app_label_sql = get_pkg_sql_info("bundles")
    main()
    print("logs_end")
    save_output_to_oss(spark)