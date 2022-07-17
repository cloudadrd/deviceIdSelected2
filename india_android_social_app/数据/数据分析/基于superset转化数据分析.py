
df = spark.sql(
    """
    select 
        gaid as device_id,
        case 
        when offer_pkg='in.mohalla.sharechat' then 0 
        when offer_pkg='in.mohalla.video' then 1 
        else 2
        end as offer_pkg_label
    from 
        ssp_log.conversion 
    where 
        pdate='20220614'
        and offer_pkg in ("in.mohalla.sharechat","in.mohalla.video","com.next.innovation.takatak")
        and country = 'IN'
        and platform='Android'
    """
)




df_conversion = spark.read.format('csv').option("header","true").load("oss://sdkemr-yeahmobi/user/chensheng/sample_ana/superset_20220614")
df_conversion.createTempView("df_conversion_table")

spark.sql(
    """
    select 
        count(device_id)
        ,count(distinct device_id)
    from 
        df_conversion_table
    """
).show(50,truncate = False)

# +----------------+-------------------------+
# |count(device_id)|count(DISTINCT device_id)|
# +----------------+-------------------------+
# |106488          |105854                   |
# +----------------+-------------------------+

df_user_bundle = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=20220613/platform=android/geo=IND")
df_user_bundle.createTempView("df_user_bundle_table")
# +------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+----+-------------+--------------+-------+----+--------------------+----+
# |ifa                                 |bundles                                                                                                                                              |osversion|ua                                                                                                                                                             |make|lastactiveday|ip            |country|lang|activedays          |type|
# +------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+----+-------------+--------------+-------+----+--------------------+----+
# |D1D0D42E-D96B-485D-A1CD-B6E1675B8C52|[freevideodownloader.allvideodownloader.hdvideodownloaderapp, com.mxtech.videoplayer.ad, com.nemo.vidmate, bubbleshooter.orig, com.rioo.runnersubway]|10.0     |Mozilla/5.0 (Linux; Android 10; CPH2239 Build/QP1A.190711.020; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/87.0.4280.101 Mobile Safari/537.36|oppo|20220503     |152.57.156.207|IND    |hi  |[60, 55, 42, 42, 41]|2   |

df_join = spark.sql(
    """
    select 
        t1.device_id
        ,t1.offer_pkg_label
        ,bundles
        ,osversion
        ,lastactiveday
        ,activedays
    from 
        df_conversion_table t1
    inner join 
        df_user_bundle_table t2
    on 
        t1.device_id = t2.ifa
    """
).persist()
df_join.createTempView("df_join_table")

df_join.repartition(1).write.mode('overwrite').orc("oss://sdkemr-yeahmobi/user/chensheng/sample_ana/superset_20220614_join")

spark.sql("""
    select
        bundles_size_label,
        count(distinct device_id) as device_id_cnt
    from
    (
        select
            device_id,
            if(size(bundles)>20,20, size(bundles)) as bundles_size_label
        from
        (
            select
                df_join_table.*
            from
                df_join_table
            inner join
                (select  device_id from (select device_id,count(offer_pkg_label) as pkg_num from df_join_table group by device_id ) t1 where pkg_num=1) t2
            on
                df_join_table.device_id = t2.device_id
        ) t3
    ) t4
    group by
        bundles_size_label
    """
).show(50,truncate=False)
df_app_info = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_app_info/")
df_app_info.createTempView("df_app_info_table")
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,ArrayType
df_join_explode = spark.sql(
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
                    device_id
                    ,bundle
                from 
                    df_join_table
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
        device_id
        ,activedays[size(activedays)-1] as days
        ,activedays
        ,lastactiveday
    from 
       df_join_table
    """
).show(50,truncate=False)

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
                device_id
                ,activedays[size(activedays)-1] as days
            from 
               df_join_table 
        ) t1
    ) t2
    group by 
        days_label 
    order by 
        device_id_cnt desc
    """
).show(60,truncate=False)









