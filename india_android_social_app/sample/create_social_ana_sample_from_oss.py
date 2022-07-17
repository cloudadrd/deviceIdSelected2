# -*- coding: utf-8 -*-
import argparse
import sys

from pyspark.sql import SparkSession
import datetime
import warnings
warnings.filterwarnings('ignore')

def sample_ana_bundles_size(table_name):
    print("sample_ana_bundles_size 分析%s的安装列表长度的分布情况"%table_name)
    spark.sql(" \
        select \
            bundles_size_label \
            ,count(distinct device_id) as device_id_cnt \
        from \
        (   \
        select  \
            device_id   \
            ,if(size(bundles)>80,80,size(bundles)) as bundles_size_label \
        from \
           %s \
        ) t1 \
        group by \
            bundles_size_label \
        order by \
            device_id_cnt desc \
    "%table_name).show(100,truncate=False)
def sample_ana_Social_ana(table_name):
    print("sample_ana_Social_ana 分析%s的Social app的安装个数"%table_name)
    spark.sql(" \
        select  \
            Social_count_label  \
            ,count(device_id) as device_id_cnt  \
        from    \
        (   \
            select  \
                device_id   \
                ,if(size(bundle_social)>20,20,size(bundle_social)) as Social_count_label    \
            from    \
                %s  \
        ) t1    \
        group by    \
            Social_count_label  \
    "%table_name).show(50,truncate=False)

def sample_ana_lastday(table_name):
    print("sample_ana_lastday 分析%s的最近一次登录距sample_date的间隔天数"%table_name)
    spark.sql(" \
        select \
            days_label \
            ,count(distinct device_id) device_id_cnt \
        from \
        ( \
            select \
                device_id \
                ,if(days>50,50,days) as days_label \
            from \
            ( \
                select \
                     device_id \
                    ,activedays[size(activedays)-1] as days \
                from \
                   %s \
            ) t1 \
        ) t2 \
        group by \
            days_label \
        order by \
            device_id_cnt desc \
        "%table_name).show(60, truncate=False)
def sample_ana_activedays_size(table_name):
    print("sample_ana_activedays_size 分析%s的activedays长度"%table_name)
    spark.sql(" \
        select \
            activedays_size_label \
            ,count(device_id) as device_id_cnt \
        from \
        ( \
            select \
                device_id \
                ,if(size(activedays_set)>80,80,size(activedays_set)) as activedays_size_label \
            from \
                %s \
        ) t1 \
        group by \
            activedays_size_label \
        order by \
            device_id_cnt desc \
        "%table_name).show(100, truncate=False)

def sample_ana_osversion(table_name):
    print("sample_ana_osversion 分析%s的系统版本信息"%table_name)
    spark.sql(" \
        select \
                osversion \
                ,count(device_id) as device_id_cnt \
            from \
               %s \
            group by \
                osversion \
            order by \
                device_id_cnt desc \
        "%table_name).show(100, truncate=False)

def sample_ana_activedays_mean(table_name):
    print("sample_ana_activedays_mean 分析%s的activedays的平均值"%table_name)
    spark.sql(" \
        select \
            activedays_mean_label \
            ,count(device_id) as device_id_cnt \
        from \
        ( \
            select \
                device_id \
                ,case \
                when activedays_mean<=0 then 1 \
                when activedays_mean<=1 then 2 \
                when activedays_mean<=2 then 3 \
                when activedays_mean<=3 then 4 \
                when activedays_mean<=5 then 5 \
                when activedays_mean<=7 then 6 \
                when activedays_mean<=9 then 7 \
                when activedays_mean<=11 then 8 \
                when activedays_mean<=13 then 9 \
                when activedays_mean<=15 then 10 \
                when activedays_mean<=20 then 11 \
                when activedays_mean<=25 then 12 \
                when activedays_mean<=30 then 13 \
                when activedays_mean<=35 then 14 \
                when activedays_mean<=40 then 15 \
                when activedays_mean<=45 then 16 \
                when activedays_mean<=50 then 17 \
                when activedays_mean<=60 then 18 \
                when activedays_mean<=70 then 19 \
                when activedays_mean<=80 then 20 \
                when activedays_mean<=90 then 21 \
                else 22 \
                end as activedays_mean_label \
            from \
            ( \
                select \
                    device_id \
                    ,avg(activeday) as activedays_mean \
                from \
                ( \
                    select \
                        device_id \
                        ,activeday \
                    from \
                        %s \
                    lateral view \
                        explode(activedays_set) as activeday \
                ) t_temp \
                group by \
                    device_id \
                     \
            ) t1 \
        ) t2 \
        group by \
            activedays_mean_label \
        order by \
            device_id_cnt desc   \
        "%table_name).show(50, truncate=False)

def get_pos_data_1(spark,sample_date,df_user_bundle_sample_date_table,df_user_bundle_sample_date_after_table,df_app_info_table):
    print("=" * 20)
    print("get_pos_data_1 开始进行正样本的初步构造")

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
    "%(df_user_bundle_sample_date_after_table,df_user_bundle_sample_date_table))
    pos_data.createTempView("pos_data_temp_table")

    pos_data_app_info = spark.sql("   \
        select  \
            device_id   \
            ,collect_set(bundle) as bundle_social   \
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
            device_id   \
        "%("pos_data_temp_table",df_app_info_table))
    pos_data_app_info.createTempView("pos_data_temp_2_table")

    pos_data_activedays = spark.sql("   \
        select  \
            device_id   \
            ,collect_set(activeday_cnt) as activedays_set    \
        from    \
        (   \
            select  \
                device_id   \
                ,concat(activeday,'_',cnt) as activeday_cnt \
            from    \
            (   \
                select  \
                    device_id,activeday \
                    ,count(device_id) as cnt    \
                from    \
                (   \
                    select  \
                        device_id   \
                        ,activeday  \
                    from    \
                        pos_data_temp_table \
                    lateral view    \
                        explode(activedays) as activeday    \
                ) t1    \
                group by    \
                    device_id,activeday \
            ) t2    \
        ) t3    \
        group by    \
            device_id")
    pos_data_activedays.createTempView("pos_data_activedays_table")
    pos_data_end = spark.sql("  \
        select  \
            t3.*    \
            ,t4.activedays_set  \
        from    \
        (   \
            select  \
                t1.*    \
                ,t2.bundle_social   \
            from    \
                %s t1   \
            left join   \
                %s t2   \
            on  \
                t1.device_id = t2.device_id \
        ) t3    \
        left join   \
            %s t4   \
        on  \
            t3.device_id = t4.device_id \
    "%("pos_data_temp_table","pos_data_temp_2_table","pos_data_activedays_table"))

    pos_data_end.repartition(50).write.mode("overwrite").orc("%s/oss_%s_pos_sample"%(oss_dirPath,sample_date))
    print("get_pos_data_1 完成正样本的初步构造")


def get_neg_data_1(spark,sample_date,neg_sample_lastdate,pos_data_table,df_user_bundle_sample_date_table,df_app_info_table):
    print("="*20)
    print("get_neg_data_1 开始进行负样本的初步构造")
    neg_data_1 = spark.sql(" \
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
        ) t1 \
        left join \
            %s t2\
        on \
            t1.device_id = t2.device_id \
        where \
            t2.device_id is null \
    "%(df_user_bundle_sample_date_table,neg_sample_lastdate,pos_data_table))
    neg_data_1.createTempView("neg_data_1_table")
    neg_data_2 = spark.sql("    \
        select  \
            device_id   \
            ,collect_set(bundle) as bundle_social   \
        from    \
        (   \
            select  \
                device_id   \
                ,bundle \
            from    \
            (   \
                select  \
                    t1.device_id    \
                    ,bundle \
                from    \
                    %s t1 \
                lateral view    \
                    explode(bundles) as bundle  \
            ) t2    \
            left join   \
                %s t3    \
            on  \
                t2.bundle = t3.id   \
            where   \
                t3.subcategory_name='Social'    \
        ) t4    \
        group by    \
            device_id   \
    "%("neg_data_1_table",df_app_info_table))
    neg_data_2.createTempView("neg_data_2_table")

    neg_data_22 = spark.sql("    \
        select  \
            *   \
        from    \
        (   \
            select  \
                t1.*    \
                ,t2.bundle_social   \
            from    \
                neg_data_1_table t1 \
            left join   \
                neg_data_2_table t2 \
            on  \
                t1.device_id = t2.device_id \
        ) t3    \
        where   \
            size(bundle_social)<=2  \
        ")
    neg_data_22.createTempView("neg_data_22_table")
    neg_data_3 = spark.sql("  \
        select  \
            tt.*    \
            ,t2.activedays_set  \
        from    \
        (   \
            select  \
                device_id   \
                ,collect_set(activeday) as activedays_set   \
            from    \
            (   \
                select  \
                    device_id   \
                    ,activeday  \
                from    \
                    neg_data_22_table    \
                lateral view    \
                    explode(activedays) as activeday    \
            )t1 \
            group by    \
                device_id   \
        ) t2    \
        right join  \
            neg_data_22_table tt \
        on  \
            tt.device_id = t2.device_id \
        ")

    neg_data_3.repartition(50).write.mode("overwrite").orc("%s/oss_%s_neg_sample"%(oss_dirPath,sample_date))

def get_neg_sample_social(spark,sample_date,neg_data_3_table,df_user_bundle_sample_date_after_table,df_app_info_table):
    print("=" * 20)
    print("get_neg_sample_social 从负样本中，抽取第二天安装Social app的数据")
    neg_data_social = spark.sql(" \
        select \
            tt.* \
            ,t4.bundle_social_2 \
        from \
            %s tt\
        left join \
        ( \
            select \
                device_id \
                ,collect_set(bundle) as bundle_social_2 \
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
                device_id \
        )  t4 \
        on \
            tt.device_id = t4.device_id \
        where   \
            size(tt.bundle_social) < size(t4.bundle_social_2) \
        "%(neg_data_3_table,df_user_bundle_sample_date_after_table,neg_data_3_table,df_app_info_table))
    neg_data_social.repartition(50).write.mode("overwrite").orc("%s/oss_%s_neg_sample_social"%(oss_dirPath,sample_date))





def main(spark,sample_date,sample_date_after,neg_sample_lastdate):
    #
    df_user_bundle_sample_date = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=%s/platform=android/geo=IND"%(sample_date))
    df_user_bundle_sample_date.createTempView("df_user_bundle_sample_date_table")
    df_user_bundle_sample_date_after = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=%s/platform=android/geo=IND"%(sample_date_after))
    df_user_bundle_sample_date_after.createTempView("df_user_bundle_sample_date_after_table")
    # 加载bundle 信息
    df_app_info = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_app_info/").persist()
    df_app_info.createTempView("df_app_info_table")
    # 正样本
    get_pos_data_1(spark, sample_date, "df_user_bundle_sample_date_table","df_user_bundle_sample_date_after_table","df_app_info_table")
    pos_data = spark.read.format('orc').load("%s/oss_%s_pos_sample"%(oss_dirPath,sample_date)).persist()
    pos_data.createTempView("pos_data_table")
    # 正样本分析
    print("main 统计pos_data_table的用户量：")
    spark.sql("select count(device_id) ,count(distinct device_id) from pos_data_table").show()
    print("main 统计各个目标app的用户量：")
    spark.sql("select target_app_label,count(device_id) ,count(distinct device_id) from pos_data_table group by target_app_label").show()
    sample_ana_bundles_size("pos_data_table")
    sample_ana_Social_ana("pos_data_table")
    sample_ana_lastday("pos_data_table")
    sample_ana_activedays_size("pos_data_table")
    sample_ana_osversion("pos_data_table")
    sample_ana_activedays_mean("pos_data_table")

    # 负样本
    get_neg_data_1(spark, sample_date,neg_sample_lastdate, "pos_data_table","df_user_bundle_sample_date_after_table","df_app_info_table")
    #
    ## 释放存
    pos_data.unpersist()

    neg_data_3 = spark.read.format("orc").load("%s/oss_%s_neg_sample"%(oss_dirPath,sample_date)).persist()
    neg_data_3.createTempView("neg_data_3_table")
    # 负样本分析
    print("main 统计负样本的用户量")
    spark.sql("select count(device_id),count(distinct device_id) from neg_data_3_table").show()
    sample_ana_bundles_size("neg_data_3_table")
    sample_ana_Social_ana("neg_data_3_table")
    sample_ana_lastday("neg_data_3_table")
    sample_ana_activedays_size("neg_data_3_table")
    sample_ana_osversion("neg_data_3_table")
    sample_ana_activedays_mean("neg_data_3_table")

    # 负样本 有安装Social app的需求分析，若 第二天 安装了Social app，则视为正样本
    get_neg_sample_social(spark, sample_date, "neg_data_3_table", "df_user_bundle_sample_date_after_table","df_app_info_table")
    neg_data_social = spark.read.format("orc").load("%s/oss_%s_neg_sample_social"%(oss_dirPath,sample_date)).persist()
    neg_data_social.createTempView("neg_data_social_table")

    # 需要将 neg_data_social 中的数据 从 neg_data_3_table 中删除
    neg_data_end = spark.sql(" \
           select  \
               t1.*    \
           from    \
               %s t1   \
           left join   \
               %s t2   \
           on  \
               t1.device_id = t2.device_id \
           where   \
               t2.device_id is null    \
           " % ("neg_data_3_table", "neg_data_social_table"))
    neg_data_end.repartition(50).write.mode("overwrite").orc("%s/oss_%s_neg_sample_2" % (oss_dirPath, sample_date))
    ## 释放内存
    neg_data_3.unpersist()

    # 分析
    sample_ana_bundles_size("neg_data_social_table")
    sample_ana_Social_ana("neg_data_social_table")
    sample_ana_lastday("neg_data_social_table")
    sample_ana_activedays_size("neg_data_social_table")
    sample_ana_osversion("neg_data_social_table")
    sample_ana_activedays_mean("neg_data_social_table")

    print("main 统计负样本，在第二天进行安装Social app的个数")
    spark.sql(" \
        select  \
            bundle_social_2_size    \
            ,count(device_id) as device_id_cnt  \
        from    \
        (   \
            select  \
                device_id   \
                ,size(bundle_social_2) as bundle_social_2_size  \
            from    \
                %s   \
        )t1 \
        group by    \
            bundle_social_2_size    \
        order by    \
            device_id_cnt desc \
    "%"neg_data_social_table").show(50,truncate=False)
    print("main 统计负样本，在第二天进行安装的Social app的种类 以及用户数")
    spark.sql(" \
        select  \
            bundle_social_2_ele \
            ,count(distinct device_id) as device_id_cnt \
        from    \
        (   \
            select  \
                device_id   \
                ,bundle_social_2_ele    \
            from    \
                %s   \
            lateral view    \
                explode(bundle_social_2) as bundle_social_2_ele \
        ) t1    \
        group by    \
            bundle_social_2_ele \
        order by    \
            device_id_cnt desc  \
    "%"neg_data_social_table").show(100,truncate=False)




if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName("sample") \
        .config("spark.sql.parquet.binaryAsString", True) \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .config("spark.default.parallelism", "1000") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample_date', help='基于用户安装列表的样本构造日期')
    args = parser.parse_args()
    print("传入参数 args",args)
    sample_date = args.sample_date
    sample_date_date = datetime.datetime.strptime(sample_date, "%Y%m%d")
    print('样本构造日期 sample_date_date',sample_date_date)
    sample_date_after = (sample_date_date + datetime.timedelta(days=1)).strftime("%Y%m%d")
    print('样本构造参考日期 sample_date_after',sample_date_after)
    now_date = datetime.datetime.now().strftime("%Y%m%d")
    print('当前日期 now_date',now_date)

    neg_sample_lastdate = (sample_date_date - datetime.timedelta(days=3)).strftime("%Y%m%d") # 当为7 时 数据量2亿，太大。计算social时内存溢出
    print('负样本筛选参考日期 neg_sample_lastdate',neg_sample_lastdate)

    oss_dirPath = 'oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app'

    # 构造样本
    if now_date < sample_date_after:
        print("当前日期<样本构造参考日期,因此无法构造样本")
        sys.exit()
    main(spark,sample_date,sample_date_after,neg_sample_lastdate)










