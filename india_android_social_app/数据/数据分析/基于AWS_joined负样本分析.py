select count(distinct user_id)  from joined where pdate='20220614' and adtype=9 and pkg in ("in.mohalla.sharechat","in.mohalla.video","com.next.innovation.takatak");
df_neg_sample = spark.sql(
    """
        select 
            distinct user_id
        from 
            ssp_log.joined 
        where 
            pdate='20220614' 
            and adtype=9 
            and pkg in ("in.mohalla.sharechat","in.mohalla.video","com.next.innovation.takatak")
    """
)
df_neg_sample.repartition(1).write.mode('overwrite').orc("s3://emr-gift-sin-bj/model/sample_ana/joined_20220614")



df_neg_sample = spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/sample_ana/aws_ssp_log_joined_data_20220614")
df_neg_sample.createTempView("df_neg_sample_table")
spark.sql("select count(distinct user_id),count(user_id) from df_neg_sample_table").show()
# +-----------------------+--------------+
# |count(DISTINCT user_id)|count(user_id)|
# +-----------------------+--------------+
# |                1303024|       1303024|
# +-----------------------+--------------+
df_pos_join=spark.read.format('orc').load("oss://sdkemr-yeahmobi/user/chensheng/sample_ana/superset_20220614_join/")
df_pos_join.createTempView("df_pos_join_table")
spark.sql("select count(distinct device_id),count(device_id) from df_pos_join_table").show()
# +-------------------------+----------------+
# |count(DISTINCT device_id)|count(device_id)|
# +-------------------------+----------------+
# |                   105208|          105837|
# +-------------------------+----------------+
df_neg_sample_filter = spark.sql("""
    select 
        df_neg_sample_table.user_id
    from 
        df_neg_sample_table
    left join 
        df_pos_join_table
    on 
        df_neg_sample_table.user_id = df_pos_join_table.device_id
    where 
        df_pos_join_table.device_id is null
""")
df_neg_sample_filter.createTempView("df_neg_sample_filter_table")
spark.sql("select count(distinct user_id),count(user_id) from df_neg_sample_filter_table").show()
# +-----------------------+--------------+
# |count(DISTINCT user_id)|count(user_id)|
# +-----------------------+--------------+
# |                1301824|       1301824|
# +-----------------------+--------------+

df_user_bundle = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=20220613/platform=android/geo=IND")
df_user_bundle.createTempView("df_user_bundle_table")
df_neg_join = spark.sql(
    """
    select 
         user_id as device_id
         ,bundles
        ,osversion
        ,lastactiveday
        ,activedays
    from 
        df_neg_sample_filter_table
    inner  join
         df_user_bundle_table
    on 
        df_neg_sample_filter_table.user_id = df_user_bundle_table.ifa
    """
).persist()
df_neg_join.createTempView("df_neg_join_table")
spark.sql("select count(device_id),count(distinct device_id) from df_neg_join_table").show()
# +--------------+-----------------------+
# |count(device_id)|count(DISTINCT device_id)|
# +--------------+-----------------------+
# |       1295721|                1295721|
# +--------------+-----------------------+
df_neg_join.repartition(10).write.mode('overwrite').orc("oss://sdkemr-yeahmobi/user/chensheng/sample_ana/aws_ssp_log_joined_data_20220614_join")


spark.sql("""
    select
        bundles_size_label,
        count(distinct device_id) as device_id_cnt
    from
    (
        select
            user_id as device_id,
            if(size(bundles)>80,80, size(bundles)) as bundles_size_label
        from
            df_neg_join_table
    ) t4
    group by
        bundles_size_label
    order by 
        device_id_cnt desc
    """
).show(100,truncate=False)
df_app_info = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_app_info/")
df_app_info.createTempView("df_app_info_table")
df_neg_join_explode = spark.sql(
    """
    select 
        Social_count_label
        ,count(distinct device_id)  as device_id_dis_cnt
    from 
    (
        select 
            device_id
            ,if(sum(Social_label)>20,20,sum(Social_label)) as Social_count_label
        from 
        (
            select 
                device_id
                ,bundle
                ,if(subcategory_name='Social',1,0) as Social_label
            from 
            (
                select 
                    user_id as device_id
                    ,bundle
                from 
                    df_neg_join_table
                lateral view
                    explode(bundles) as bundle
            ) t1
            inner join 
                df_app_info_table
            on 
                t1.bundle = df_app_info_table.id
            where 
                df_app_info_table.subcategory_name is not null
        ) t2
        group by 
            device_id
    ) t3 
    group by 
        Social_count_label
    order by 
        device_id_dis_cnt desc 
    """
).show(50,truncate = False)

spark.sql(
    """
    select 
        days_label
        ,count(distinct device_id) device_id_cnt
    from
    (
        select
            device_id
            ,if(days>50,50,days) as days_label
        from 
        (
            select 
                user_id as device_id
                ,activedays[size(activedays)-1] as days
            from 
               df_neg_join_table 
        ) t1
    ) t2
    group by 
        days_label 
    order by 
        device_id_cnt desc
    """
).show(60,truncate=False)
df_neg_all = spark.sql(
    """
    select 
        *
    from 
        
    """
)










