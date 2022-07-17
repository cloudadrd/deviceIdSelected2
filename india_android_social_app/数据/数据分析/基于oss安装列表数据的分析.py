df_user_bundle_20220613 = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=20220613/platform=android/geo=IND")
df_user_bundle_20220613.createTempView("df_user_bundle_20220613_table")
df_user_bundle_20220614 = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=20220614/platform=android/geo=IND")
df_user_bundle_20220614.createTempView("df_user_bundle_20220614_table")
pos_data = spark.sql("""
    select 
        t2.device_id
        ,bundles
        ,osversion
        ,lastactiveday
        ,activedays
        ,t1.target_app_label
    from 
    (
        select 
            ifa as device_id
            ,case 
             when array_contains(bundles,'com.next.innovation.takatak') then 1 
             when array_contains(bundles,'in.mohalla.video') then 2
             when array_contains(bundles,'in.mohalla.sharechat') then 3
             else 0
             end as target_app_label 
        from 
            df_user_bundle_20220614_table
        where 
            array_contains(bundles,'com.next.innovation.takatak') or array_contains(bundles,'in.mohalla.video') or array_contains(bundles,'in.mohalla.sharechat')
    ) t1
    right join
    (
        select 
            ifa as device_id
            ,bundles
            ,osversion
            ,lastactiveday
            ,activedays
        from 
            df_user_bundle_20220613_table
        where 
            not (array_contains(bundles,'com.next.innovation.takatak') or array_contains(bundles,'in.mohalla.video') or array_contains(bundles,'in.mohalla.sharechat'))
    ) t2 
    on 
        t1.device_id = t2.device_id
    where 
        t1.device_id is not null
        and t1.target_app_label != 0
""").persist()
pos_data.createTempView("pos_data_table")
spark.sql("select count(device_id) ,count(distinct device_id) from pos_data_table").show()
spark.sql("select target_app_label,count(device_id) ,count(distinct device_id) from pos_data_table group by target_app_label").show()
# +----------------+----------------+-------------------------+
# |target_app_label|count(device_id)|count(DISTINCT device_id)|
# +----------------+----------------+-------------------------+
# |               1|           42267|                    42267|
# |               3|          232085|                   232085|
# |               2|          129827|                   129827|
# +----------------+----------------+-------------------------+
pos_data.repartition(50).write.mode("overwrite").orc("oss://sdkemr-yeahmobi/user/chensheng/sample_ana/oss_20220613_pos_sample")
# +----------------+-------------------------+
# |count(device_id)|count(DISTINCT device_id)|
# +----------------+-------------------------+
# |          404179|                   404179|
# +----------------+-------------------------+

# 1.针对 安装列表
spark.sql("""
    select 
        bundles_size_label
        ,count(distinct device_id) as device_id_cnt
    from 
    (
    select 
        device_id
        ,if(size(bundles)>80,80,size(bundles)) as bundles_size_label
    from 
       pos_data_table 
    ) t1 
    group by 
        bundles_size_label
    order by 
        device_id_cnt desc
""").show(100,truncate=False)
# 2.针对安装Social
df_app_info = spark.read.format("orc").load("oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_app_info/")
df_app_info.createTempView("df_app_info_table")
pos_data_table_explode = spark.sql(
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
                    pos_data_table
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
# 最后一次登录情况
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
               pos_data_table 
        ) t1
    ) t2
    group by 
        days_label 
    order by 
        device_id_cnt desc
    """
).show(60,truncate=False)
# 正样本活跃天数
spark.sql(
    """
    select 
        activedays_size_label
        ,count(device_id) as device_id_cnt
    from
    (
        select 
            device_id
            ,if(size(activedays)>80,80,size(activedays)) as activedays_size_label 
        from 
            pos_data_table
    ) t1
    group by 
        activedays_size_label
    order by 
        device_id_cnt desc
    """
).show(100,truncate=False)
# 正样本版本信息
spark.sql(
    """
    select 
            osversion
            ,count(device_id) as device_id_cnt
        from 
            pos_data_table
        group by 
            osversion
        order by 
            device_id_cnt desc
    """
).show(100,truncate=False)
# 正样本activedays mean
from pyspark.sql.functions import udf,mean,size
from pyspark.sql.types import StringType,ArrayType,IntegerType,DoubleType
pos_data = spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/sample_ana/oss_20220613_pos_sample")
pos_data.createTempView("pos_data_table")
spark.sql("select count(device_id) from pos_data_table where activedays is null").show()

spark.sql(
    """
    select 
        activedays_mean_label
        ,count(device_id) as device_id_cnt
    from 
    (
        select 
            device_id
            ,case 
            when activedays_mean<=5 then 1 
            when activedays_mean<=10 then 2
            when activedays_mean<=15 then 3 
            when activedays_mean<=20 then 4 
            when activedays_mean<=25 then 5
            when activedays_mean<=30 then 6
            when activedays_mean<=35 then 7
            when activedays_mean<=40 then 8
            when activedays_mean<=45 then 9
            when activedays_mean<=50 then 10
            else 11
            end as activedays_mean_label
        from 
        (   
            select 
                device_id
                ,avg(activeday) as activedays_mean
            from 
            (
                select 
                    device_id
                    ,activeday
                from 
                    pos_data_table
                lateral view
                    explode(activedays) as activeday 
            ) t_temp
            group by 
                device_id
        
        ) t1
    ) t2
    group by 
        activedays_mean_label
    order by 
        device_id_cnt desc  
    """
).show(50,truncate=False)


#    构造负样本 （安装列表长度<=20,Social_app安装个数0,最后一次登录 20220613）
spark.sql(
    """
    select 
        lastactiveday
        ,count(distinct device_id) as device_id_cnt
    from 
        pos_data_table
    group by 
        lastactiveday
    order by 
        device_id_cnt desc
    """
).show(50)

neg_data_1 = spark.sql("""
    select 
        t1.*
    from 
    (
        select 
            ifa as device_id
            ,bundles
            ,osversion
            ,lastactiveday
            ,activedays
        from 
            df_user_bundle_20220613_table
        where 
            size(bundles)<=20
            and lastactiveday='20220613'
    ) t1
    left join 
        pos_data_table
    on 
        t1.device_id = pos_data_table.device_id
    where 
        pos_data_table.device_id is null
""")
neg_data_1.createTempView("neg_data_1_table")
neg_data_2 = spark.sql("""
    select 
        device_id
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
                    neg_data_1_table
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
    where 
        Social_count_label = 0
""")
neg_data_2.createTempView("neg_data_2_table")
neg_data_3  = spark.sql(
    """
    select 
        neg_data_1_table.*
    from 
        neg_data_1_table
    inner join 
        neg_data_2_table
    on  
       neg_data_1_table.device_id =  neg_data_2_table.device_id
    """
).persist()
neg_data_3.createTempView("neg_data_3_table")
spark.sql("select count(device_id),count(distinct device_id) from neg_data_3_table").show()
neg_data_3.repartition(50).write.mode("overwrite").orc("oss://sdkemr-yeahmobi/user/chensheng/sample_ana/oss_20220613_neg_sample")
# 负样本 安装列表长度
spark.sql("""
    select 
        bundles_size_label
        ,count(device_id) as device_id_cnt
    from
    (
        select 
            device_id
            ,if(size(bundles)>80,80,size(bundles)) as bundles_size_label 
        from 
            neg_data_3_table
    ) t1
    group by 
        bundles_size_label
    order by 
        device_id_cnt desc
""").show(100,truncate=False)
# 负样本 活跃天数
spark.sql(
    """
    select 
        activedays_size_label
        ,count(device_id) as device_id_cnt
    from
    (
        select 
            device_id
            ,if(size(activedays)>80,80,size(activedays)) as activedays_size_label 
        from 
            neg_data_3_table
    ) t1
    group by 
        activedays_size_label
    order by 
        device_id_cnt desc
    """
).show(100,truncate=False)
# 负样本版本信息
spark.sql(
    """
    select 
            osversion
            ,count(device_id) as device_id_cnt
        from 
            neg_data_3_table
        group by 
            osversion
        order by 
            device_id_cnt desc
    """
).show(100,truncate=False)
# 针对负样本，若第二天有安装Social则为0，负否为1
neg_data_social = spark.sql(
    """
    select 
        neg_data_3_table.*
        ,t4.Social_label_count
    from 
        neg_data_3_table
    left join 
    (
        select 
            device_id
            ,sum(Social_label) as Social_label_count
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
                (
                    select 
                        df_user_bundle_20220614_table.ifa as device_id
                        ,df_user_bundle_20220614_table.bundles
                    from    
                        df_user_bundle_20220614_table
                    inner join 
                        neg_data_3_table
                    on  
                        df_user_bundle_20220614_table.ifa = neg_data_3_table.device_id
                )  t1
                lateral view
                    explode(bundles) as bundle
            ) t2 
            inner join 
                df_app_info_table
            on 
                t2.bundle = df_app_info_table.id
            where 
                df_app_info_table.subcategory_name is not null
        ) t3
        group by 
            device_id   
    )  t4 
    on 
        neg_data_3_table.device_id = t4.device_id
    """
).persist()
neg_data_social.repartition(50).write.mode("overwrite").orc("oss://sdkemr-yeahmobi/user/chensheng/sample_ana/oss_20220613_neg_sample_social")

neg_data_social.createTempView("neg_data_social_table")
spark.sql("""
    select 
        Social_label_count
        ,count(device_id) as device_id_cnt
    from 
        neg_data_social_table
    group by 
        Social_label_count
    order by 
        device_id_cnt
""").show(50,truncate=False)
# +------------------+-------------+
# |Social_label_count|device_id_cnt|
# +------------------+-------------+
# |5                 |3            |
# |4                 |9            |
# |3                 |61           |
# |2                 |789          |
# |1                 |54374        |
# |null              |159385       |
# |0                 |80616405     |
# +------------------+-------------+
spark.sql("select bundles from neg_data_social_table where Social_label_count is null limit 50").show(50,truncate=False)

#
neg_data_social = spark.read.format('orc').load("oss://sdkemr-yeahmobi/user/chensheng/sample_ana/oss_20220613_neg_sample_social")

neg_data_social.createTempView("neg_data_social_table")

spark.sql(
    """
    select 
        activedays_mean_label
        ,count(device_id) as device_id_cnt
    from 
    (
        select 
            device_id
            ,case 
            when activedays_mean<=5 then 1 
            when activedays_mean<=10 then 2
            when activedays_mean<=15 then 3 
            when activedays_mean<=20 then 4 
            when activedays_mean<=25 then 5
            when activedays_mean<=30 then 6
            when activedays_mean<=35 then 7
            when activedays_mean<=40 then 8
            when activedays_mean<=45 then 9
            when activedays_mean<=50 then 10
            else 11
            end as activedays_mean_label
        from 
        (   
            select 
                device_id
                ,avg(activeday) as activedays_mean
            from 
            (
                select 
                    device_id
                    ,activeday
                from 
                    neg_data_social_table
                lateral view
                    explode(activedays) as activeday 
            ) t_temp
            group by 
                device_id
        
        ) t1
    ) t2
    group by 
        activedays_mean_label
    order by 
        device_id_cnt desc  
    """
).show(50,truncate=False)

# 统计neg_data_social_table Social_label_count=1 对应的app
neg_data_social_1 = spark.sql("""
    select 
        device_id
        ,bundle
        ,subcategory_name
    from 
    (
        select 
            device_id
            ,bundle
            ,osversion
            ,lastactiveday
            ,activedays
        from 
        (
            select 
                df_user_bundle_20220614_table.ifa as device_id
                ,bundles
                ,osversion
                ,lastactiveday
                ,activedays
            from 
            (
                select 
                   distinct  device_id
                from 
                    neg_data_social_table
                where 
                    Social_label_count=1
            ) t1
            inner join 
                df_user_bundle_20220614_table
            on 
                t1.device_id = df_user_bundle_20220614_table.ifa
        ) t2
        lateral view
            explode(bundles) as bundle
    ) t3 
    inner join
         df_app_info_table
    on 
        t3.bundle = df_app_info_table.id
    where 
        df_app_info_table.subcategory_name is not null
        and df_app_info_table.subcategory_name = 'Social'
""").persist()
neg_data_social_1.createTempView("neg_data_social_1_table")
spark.sql("select bundle,count(device_id) as device_id_cnt from neg_data_social_1_table group by bundle order by device_id_cnt desc").show(50,truncate=False)
# +-----------------------------------------------------------+-------------+
# |bundle                                                     |device_id_cnt|
# +-----------------------------------------------------------+-------------+
# |instagram.video.downloader.story.saver                     |8370         |
# |com.cardfeed.video_public                                  |7215         |
# |com.eterno.shortvideos                                     |5394         |
# |sun.way2sms.hyd.com                                        |2224         |
# |com.lazygeniouz.saveit                                     |2136         |
# |com.blockchainvault                                        |1529         |
# |get.lokal.localnews                                        |1509         |
# |com.vivashow.share.video.chat                              |1390         |
# |com.bela.live                                              |1127         |
# |com.dianyun.chikii                                         |1050         |
# |com.bunny.pro                                              |1005         |
# |com.halo.pro                                               |915          |
# |com.facechat.live                                          |727          |
# |com.magic.whatsapp.status.saver.download                   |691          |
# |cn.urhoney.u                                               |666          |
# |com.grindrapp.android                                      |598          |
# |photo.video.instasaveapp                                   |544          |
# |com.laki.live                                              |543          |
# |me.dingtone.app.im                                         |494          |
# |com.exutech.chacha                                         |494          |
# |com.indownloder.rockersapp                                 |487          |
# |com.mosin.lee.gbversion.latest                             |485          |
# |com.photophone.callerscreen.phonecalllocator               |475          |
# |com.meeta.live                                             |441          |
# |com.blued.international                                    |424          |
# |io.chingari.app                                            |403          |
# |com.domobile.messenger                                     |387          |
# |com.kkmversion.thelightv                                   |375          |
# |com.gbpro.whtsappna                                        |276          |
# |com.thinkmobile.accountmaster                              |251          |
# |com.tweetcreator.fktweeteditor                             |245          |
# |com.whazappstatus.whasappstatus                            |240          |
# |com.khushwant.sikhworld                                    |238          |
# |com.belalite.live                                          |237          |
# |com.gogii.textplus                                         |237          |
# |com.mogul.flutte                                           |231          |
# |com.piko.live                                              |223          |
# |statussaver.statusdownloader.downloadstatus.videoimagesaver|209          |
# |com.zekichat.live                                          |207          |
# |com.gagalite.live                                          |203          |
# |com.stackwares.android.storysaver.facebook                 |203          |
# |save.videos.officially                                     |191          |
# |com.tumblr                                                 |170          |
# |com.brownhatlabs.reels.downloader                          |168          |
# |com.voicemaker.android                                     |162          |
# |messenger.chat.social.messenger                            |151          |
# |videomaker.slideshow.editor.photo.editor                   |145          |
# |com.amipaappsstudio.videochat.videocall.talk               |136          |
# |com.brownhatlabs.whatsapp_status_downloader                |127          |
# |com.polestar.super.clone                                   |124          |
# +-----------------------------------------------------------+-------------+

spark.sql("select count(device_id) from neg_data_social_1_table").show()
neg_data_social_1.repartition(1).write.mode("overwrite").orc("oss://sdkemr-yeahmobi/user/chensheng/sample_ana/oss_20220613_neg_sample_social_1")

neg_data_social_1= spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/sample_ana/oss_20220613_neg_sample_social_1")


def test():
    neg_data_social_1 = spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/sample_ana/oss_20220613_neg_sample_social_1")
    neg_data_social_1.createTempView("neg_data_social_1_table")
    print("123")
spark.sql("select count(device_id) from neg_data_social_1_table").show()




