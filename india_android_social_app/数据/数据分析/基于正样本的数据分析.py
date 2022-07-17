from  pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,ArrayType
def load_sample():
    positive_data = spark.read.format("orc").load("s3://emr-gift-sin-bj/fm_feature_data/train/day=20220531")
    positive_data.createTempView("positive_data_table")
    spark.sql("desc positive_data_table").show(truncate=False)
    # +-------------------+-------------------------------------------------------------------------------+-------+
    # | col_name | data_type | comment |
    # +-------------------+-------------------------------------------------------------------------------+-------+
    # | device_id | string | null |
    # | user_bundle_list | array < string > | null |
    # | int_tag | string | null |
    # | source_tag | string | null |
    # | country | string | null |
    # | os_ver_name | string | null |
    # | language | string | null |
    # | install_bundle_size | int | null |
    # | lastactiveday | string | null |
    # | latest3_bundle_info | array < struct < bundle: string, maincategory: string, subcategory: string, size: bigint >> | null |
    # +-------------------+-------------------------------------------------------------------------------+-------+
    spark.sql("select count(distinct device_id) from positive_data_table").show(truncate=False) # 3222665
    spark.sql("select count(*) from positive_data_table").show(truncate=False) # 3222665
    spark.sql("select * from positive_data_table limit 1").show(truncate=False)
    # 筛选安装列表中含有指定 app 的 用户 （"com.next.innovation.takatak", "in.mohalla.sharechat", "in.mohalla.video"）
    positive_data_app = spark.sql(
        """
        select
            *
        from 
            positive_data_table
        where 
            array_contains(user_bundle_list,'com.next.innovation.takatak')
            or 
            array_contains(user_bundle_list,'in.mohalla.sharechat')
            or
            array_contains(user_bundle_list,'in.mohalla.video')
         """
    )
    positive_data_app.createTempView("positive_data_app_table")
    spark.sql("""
        select 
            device_id,user_bundle,count(*) as app_repeat_count
        from 
        (
            select 
                device_id,user_bundle
            from 
                positive_data_app_table
            lateral view 
                explode(user_bundle_list)  as user_bundle
        ) t1 
        group by 
            device_id,user_bundle
        order by 
            app_repeat_count desc,device_id,user_bundle
    """).show(50,truncate=False)
#     +------------------------------------+---------------------------------------------------+----------------+
# |device_id                           |user_bundle                                        |app_repeat_count|
# +------------------------------------+---------------------------------------------------+----------------+
# |000009EF-67DF-41AC-A974-344BF084C35E|com.mi.android.globalminusscreen                   |1               |
# |000009EF-67DF-41AC-A974-344BF084C35E|com.miui.msa.global                                |1               |
# |000009EF-67DF-41AC-A974-344BF084C35E|com.miui.player                                    |1               |
# |000009EF-67DF-41AC-A974-344BF084C35E|com.mxtech.videoplayer.ad                          |1               |
# |000009EF-67DF-41AC-A974-344BF084C35E|com.snaptube.premium                               |1               |
# |000009EF-67DF-41AC-A974-344BF084C35E|com.truecaller                                     |1               |
# |000009EF-67DF-41AC-A974-344BF084C35E|com.xiaomi.midrop                                  |1               |
# |000009EF-67DF-41AC-A974-344BF084C35E|com.xiaomi.mipicks                                 |1               |
# |000009EF-67DF-41AC-A974-344BF084C35E|in.mohalla.sharechat                               |1               |
# |00000EC0-2CB1-446C-A8E7-80BFF4A4FFCF|com.imo.android.imoim                              |1               |
# |00000EC0-2CB1-446C-A8E7-80BFF4A4FFCF|com.xiaomi.mipicks                                 |1               |
# |00000EC0-2CB1-446C-A8E7-80BFF4A4FFCF|in.mohalla.video                                   |1               |
# |00000EC4-F635-4803-B738-34B99E2216D9|bubble.shoot.fruit.splash.game2                    |1               |
# |00000EC4-F635-4803-B738-34B99E2216D9|com.eterno                                         |1               |
# |00000EC4-F635-4803-B738-34B99E2216D9|com.mi.android.globalminusscreen                   |1               |
# |00000EC4-F635-4803-B738-34B99E2216D9|com.miui.msa.global                                |1               |
# |00000EC4-F635-4803-B738-34B99E2216D9|com.nordcurrent.canteenhd                          |1               |
# |00000EC4-F635-4803-B738-34B99E2216D9|com.one1line.onetouch.onestroke.dotgame            |1               |
# |00000EC4-F635-4803-B738-34B99E2216D9|com.xiaomi.mipicks                                 |1               |
# |00000EC4-F635-4803-B738-34B99E2216D9|in.mohalla.video                                   |1               |
# |00000EC4-F635-4803-B738-34B99E2216D9|net.zedge.android                                  |1               |
# |00000EC4-F635-4803-B738-34B99E2216D9|whatsapp.status.sver                               |1               |
# |00001268-F0E8-4BC2-952E-B1EE8AD284A5|com.crazy.plane.landing                            |1               |
# |00001268-F0E8-4BC2-952E-B1EE8AD284A5|com.mxtech.videoplayer.ad                          |1               |
# |00001268-F0E8-4BC2-952E-B1EE8AD284A5|com.next.innovation.takatak                        |1               |
# |00001268-F0E8-4BC2-952E-B1EE8AD284A5|com.showtimeapp                                    |1               |
# |00001268-F0E8-4BC2-952E-B1EE8AD284A5|com.trustedapp.pdfreaderpdfviewer                  |1               |
# |00001268-F0E8-4BC2-952E-B1EE8AD284A5|com.xiaomi.midrop                                  |1               |
# |0000135E-B45F-440F-B47A-AB2E8822506C|com.miui.msa.global                                |1               |
# |0000135E-B45F-440F-B47A-AB2E8822506C|com.next.innovation.takatak                        |1               |
# |0000135E-B45F-440F-B47A-AB2E8822506C|com.xiaomi.mipicks                                 |1               |
# |0000135E-B45F-440F-B47A-AB2E8822506C|in.mohalla.sharechat                               |1               |
# |00001518-FAF2-49F6-8E1D-49570108558A|com.amanotes.beathopper                            |1               |
# |00001518-FAF2-49F6-8E1D-49570108558A|com.linecorp.b612.android                          |1               |
# |00001518-FAF2-49F6-8E1D-49570108558A|com.oppo.market                                    |1               |
# |00001518-FAF2-49F6-8E1D-49570108558A|in.mohalla.video                                   |1               |
# |00001729-06AE-4F9D-82C2-F939191872BB|com.funnypuri.client                               |1               |
# |00001729-06AE-4F9D-82C2-F939191872BB|com.next.innovation.takatak                        |1               |
# |00001729-06AE-4F9D-82C2-F939191872BB|com.rioo.runnersubway                              |1               |
# |00002247-2AB5-4AFA-9038-81EED0319E83|com.android.thememanager                           |1               |
# |00002247-2AB5-4AFA-9038-81EED0319E83|com.fast.free.unblock.secure.vpn                   |1               |
# |00002247-2AB5-4AFA-9038-81EED0319E83|com.mi.android.globalminusscreen                   |1               |
# |00002247-2AB5-4AFA-9038-81EED0319E83|com.miui.cleanmaster                               |1               |
# |00002247-2AB5-4AFA-9038-81EED0319E83|com.miui.global.packageinstaller                   |1               |
# |00002247-2AB5-4AFA-9038-81EED0319E83|com.miui.msa.global                                |1               |
# |00002247-2AB5-4AFA-9038-81EED0319E83|com.sukhavati.gotoplaying.bubble.BubbleShooter.mint|1               |
# |00002247-2AB5-4AFA-9038-81EED0319E83|com.xiaomi.mipicks                                 |1               |
# |00002247-2AB5-4AFA-9038-81EED0319E83|in.mohalla.video                                   |1               |
# |00002260-1D6C-4180-B1D5-E27BEBA518E7|GBWhatsAppAPK                                      |1               |
# |00002260-1D6C-4180-B1D5-E27BEBA518E7|audio.effect.music.equalizer.musicplayer           |1               |
# +------------------------------------+---------------------------------------------------+----------------+
    spark.sql(
        """
        select 
            contains_num,count(device_id) as device_id_cnt
        from 
        (
            select 
                device_id,contains_1+contains_2+contains_3 as contains_num
            from 
            (
                select 
                    device_id
                    ,user_bundle_list
                    ,if(array_contains(user_bundle_list,'com.next.innovation.takatak'),1,0) as contains_1 
                    ,if(array_contains(user_bundle_list,'in.mohalla.sharechat'),1,0) as contains_2
                    ,if(array_contains(user_bundle_list,'in.mohalla.video'),1,0) as contains_3 
                from 
                    positive_data_app_table
            ) t1
        )t2 
        group by 
            contains_num
        """
    ).show(50,truncate=False)
    # +------------+-------------+
    # |contains_num|device_id_cnt|
    # +------------+-------------+
    # |1           |2603369      |
    # |3           |40093        |
    # |2           |579203       |
    # +------------+-------------+





def load_app_info():
    # 针对所有正样本的app_list 安照类别统计个数
    app_info = spark.read.format("orc").load("s3://emr-gift-sin-bj/model/app_info/")
    app_info.createTempView("app_info_table")
    spark.sql("desc app_info_table").show(truncate=False)
    # +------------------------+-------------+-------+
    # | col_name | data_type | comment |
    # +------------------------+-------------+-------+
    # | id | string | null |
    # | compatible_ios | string | null |
    # | publisher_id | bigint | null |
    # | category_name | string | null |
    # | other_stores | array < string > | null |
    # | description | string | null |
    # | subcategory_name | string | null |
    # | subcategory_id | int | null |
    # | apptopia_url | string | null |
    # | category_id | int | null |
    # | offers_in_app_purchases | boolean | null |
    # | permissions | array < string > | null |
    # | privacy_url | string | null |
    # | category_ids | array < int > | null |
    # | age_restrictions | string | null |
    # | cross_store_publisher_id | bigint | null |
    # | last_update_date | string | null |
    # | approx_size_bytes | bigint | null |
    # | screenshot_urls | array < string > | null |
    # | icon_url | string | null |
    # +------------------------+-------------+-------+
    spark.sql(
        """
        select 
            id,count(*) as id_cnt
        from 
            app_info_table
        group by 
            id
        order by 
            id_cnt desc,id
        """
    ).show(5,truncate=False)
@udf(ArrayType(StringType()))
def get_bundles_index(bundles):
    bundles_index_list=[]
    for i in range(len(bundles)):
        bundles_index_list.append(str(bundles[i])+"_"+str(i))
    return bundles_index_list
def sample_app_info():
    positive_data_explode_app_info = spark.sql("""
                    select 
                        device_id,int_tag,source_tag,country,os_ver_name,language,lastactiveday,user_bundle,
                        category_name,subcategory_name,approx_size_bytes
                    from 
                    (
                        select 
                            device_id,int_tag,source_tag,country,os_ver_name,language,lastactiveday,user_bundle
                        from 
                            positive_data_table
                        lateral view 
                            explode(user_bundle_list)  as user_bundle
                    ) t1 
                    inner join 
                        app_info_table
                    on 
                        t1.user_bundle= app_info_table.id
            """)
    positive_data_explode_app_info.createTempView("positive_data_explode_app_info_table")
    ana_category = spark.sql(
        """
        select 
             category_cnt_label,category_name,subcategory_name,count(device_id) as device_id_cnt
        from 
        (
            select
                device_id,category_name,subcategory_name,
                if(category_cnt<=10,category_cnt,10) as category_cnt_label
            from 
            (
                select 
                    device_id,category_name,subcategory_name,count(*) as category_cnt
                from 
                    positive_data_explode_app_info_table
                group by 
                    device_id,category_name,subcategory_name
            )
        ) t1 
        group by 
            category_cnt_label,category_name,subcategory_name
        order by 
            device_id_cnt desc,category_cnt_label,category_name,subcategory_name
        """
    ).persist()
    ana_category.createTempView("ana_category_table")
    # 1.分析安装social 软件个数的用户占比
    spark.sql(
        """
        select 
            category_cnt_label,category_name,subcategory_name, device_id_cnt
        from 
            ana_category_table
        where
            subcategory_name == 'Social'
        order by 
            device_id_cnt desc,category_cnt_label,category_name,subcategory_name
        """
    ).show(200,truncate=False)
    # +------------------+-------------+----------------+-------------+
    # | category_cnt_label | category_name | subcategory_name | device_id_cnt |
    # +------------------+-------------+----------------+-------------+
    # | 1 | Applications | Social | 2404466 |
    # | 2 | Applications | Social | 690395 |
    # | 3 | Applications | Social | 106467 |
    # | 4 | Applications | Social | 13979 |
    # | 5 | Applications | Social | 3289 |
    # | 6 | Applications | Social | 1535 |
    # | 7 | Applications | Social | 934 |
    # | 10 | Applications | Social | 653 |
    # | 8 | Applications | Social | 582 |
    # | 9 | Applications | Social | 365 |
    # +------------------+-------------+----------------+-------------+

    # 针对社交Socail 的 app 2404466/3222665 = 74% 的用户只安装了一个软件， 690395/3222665 = 21% 的用户安装了两个软件


    # 2.针对social 在安装序列中的位置进行分析
    positive_data_2 = positive_data.select("device_id",get_bundles_index("user_bundle_list").alias("user_bundle_list_2")).persist()
    positive_data_2.createTempView("positive_data_3_table")
    # spark.sql("select * from positive_data_3_table limit 10").show(20,truncate=False)
    positive_data_2_app_info = spark.sql(
        """
        select 
            device_id,bundle,index,subcategory_name,
            if(index<20,index,20) as index_label
        from 
        (
            select 
                device_id,bundle,index,subcategory_name
            from 
            (
                select 
                    device_id
                    ,split(bundle_index,'_')[0] as bundle
                    ,split(bundle_index,'_')[1] as index
                from 
                    positive_data_3_table
                lateral view 
                    explode(user_bundle_list_2)  as bundle_index
            ) t1 
            inner join 
                app_info_table
            on 
                t1.bundle = app_info_table.id
            where 
                subcategory_name = 'Social'
        ) t2 
        
        """
    ).persist()
    positive_data_2_app_info.createTempView("positive_data_2_app_info_table")
    # 针对 仅安装一个社交软件的用户进行分析
    spark.sql("""
        select 
            bundle,index_label,count(device_id) as index_label_count
        from 
        (
            select 
                positive_data_2_app_info_table.device_id,
                bundle,index,subcategory_name,index_label
            from 
            (
                select 
                    device_id
                from 
                    positive_data_2_app_info_table
                group by 
                    device_id
                having 
                    count(device_id)=1
            ) t1
            left join
                positive_data_2_app_info_table
            on 
                t1.device_id =  positive_data_2_app_info_table.device_id
        ) t2 
        group by 
            bundle,index_label
        order by 
            index_label_count desc,bundle,index_label
    """).show(500,truncate=False)
# +---------------------------+-----------+-----------------+
# |bundle                     |index_label|index_label_count|
# +---------------------------+-----------+-----------------+
# |in.mohalla.video           |1          |143957           |
# |in.mohalla.sharechat       |2          |133221           |
# |in.mohalla.sharechat       |1          |132359           |
# |in.mohalla.video           |2          |128414           |
# |in.mohalla.video           |0          |110951           |
# |in.mohalla.sharechat       |3          |108432           |
# |in.mohalla.video           |3          |99944            |
# |in.mohalla.video           |20         |98644            |
# |in.mohalla.sharechat       |0          |86400            |
# |com.next.innovation.takatak|1          |86343            |
# |in.mohalla.sharechat       |4          |86067            |
# |com.next.innovation.takatak|2          |83860            |
# |in.mohalla.video           |4          |79632            |
# |in.mohalla.sharechat       |5          |68640            |
# |com.next.innovation.takatak|3          |67609            |
# |in.mohalla.video           |5          |64117            |
# |com.next.innovation.takatak|0          |56432            |
# |com.next.innovation.takatak|4          |54372            |
# |in.mohalla.sharechat       |6          |53624            |
# |in.mohalla.video           |6          |51895            |
# |com.next.innovation.takatak|20         |46771            |
# |in.mohalla.sharechat       |20         |44435            |
# |com.next.innovation.takatak|5          |44203            |
# |in.mohalla.sharechat       |7          |41660            |
# |in.mohalla.video           |7          |41161            |
# |com.next.innovation.takatak|6          |35481            |
# |in.mohalla.video           |8          |32145            |
# |in.mohalla.sharechat       |8          |30909            |
# |com.next.innovation.takatak|7          |27897            |
# |in.mohalla.video           |9          |24349            |
# |in.mohalla.sharechat       |9          |21668            |
# |com.next.innovation.takatak|8          |21067            |
# |in.mohalla.video           |10         |18743            |
# |com.next.innovation.takatak|9          |15408            |
# |in.mohalla.video           |11         |15154            |
# |in.mohalla.sharechat       |10         |14618            |
# |in.mohalla.video           |12         |13082            |
# |in.mohalla.video           |13         |11460            |
# |com.next.innovation.takatak|10         |10922            |
# |in.mohalla.video           |14         |10600            |
# |in.mohalla.sharechat       |11         |10129            |
# |in.mohalla.video           |15         |9812             |
# |in.mohalla.video           |16         |8917             |
# |com.next.innovation.takatak|11         |8381             |
# |in.mohalla.video           |17         |7911             |
# |in.mohalla.sharechat       |12         |7801             |
# |in.mohalla.video           |18         |7300             |
# |com.next.innovation.takatak|12         |6995             |
# |in.mohalla.video           |19         |6664             |
# |in.mohalla.sharechat       |13         |6505             |
# |com.next.innovation.takatak|13         |6252             |
# |in.mohalla.sharechat       |14         |5696             |
# |com.next.innovation.takatak|14         |5583             |
# |com.next.innovation.takatak|15         |5002             |
# |in.mohalla.sharechat       |15         |4817             |
# |com.next.innovation.takatak|16         |4450             |
# |in.mohalla.sharechat       |16         |4232             |
# |com.next.innovation.takatak|17         |4060             |
# |in.mohalla.sharechat       |17         |3909             |
# |com.next.innovation.takatak|18         |3615             |
# |in.mohalla.sharechat       |18         |3532             |
# |com.next.innovation.takatak|19         |3397             |
# |in.mohalla.sharechat       |19         |3048             |
# +---------------------------+-----------+-----------------+

    # 针对 安装两个社交软件的用户进行分析
    spark.sql("""
            select 
                bundle,index_label,count(device_id) as index_label_count
            from 
            (
                select 
                    positive_data_2_app_info_table.device_id,
                    bundle,index,subcategory_name,index_label
                from 
                (
                    select 
                        device_id
                    from 
                        positive_data_2_app_info_table
                    group by 
                        device_id
                    having 
                        count(device_id)=2
                ) t1
                left join
                    positive_data_2_app_info_table
                on 
                    t1.device_id =  positive_data_2_app_info_table.device_id
            ) t2 
            group by 
                bundle,index_label
            order by 
                index_label_count desc,bundle,index_label
        """).show(500, truncate=False)


# +---------------------------------------------------------------------+-----------+-----------------+
# |bundle                                                               |index_label|index_label_count|
# +---------------------------------------------------------------------+-----------+-----------------+
# |in.mohalla.video                                                     |1          |60398            |
# |in.mohalla.sharechat                                                 |2          |59888            |
# |in.mohalla.video                                                     |2          |58796            |
# |in.mohalla.sharechat                                                 |3          |55256            |
# |in.mohalla.sharechat                                                 |1          |53769            |
# |in.mohalla.video                                                     |3          |50989            |
# |in.mohalla.video                                                     |0          |50481            |
# |in.mohalla.sharechat                                                 |0          |50171            |
# |in.mohalla.sharechat                                                 |4          |48050            |
# |in.mohalla.video                                                     |4          |43410            |
# |in.mohalla.sharechat                                                 |5          |40132            |
# |com.next.innovation.takatak                                          |2          |37844            |
# |com.next.innovation.takatak                                          |1          |36841            |
# |in.mohalla.video                                                     |5          |36698            |
# |com.next.innovation.takatak                                          |3          |33561            |
# |in.mohalla.sharechat                                                 |6          |32975            |
# |in.mohalla.video                                                     |20         |30408            |
# |in.mohalla.video                                                     |6          |29310            |
# |com.next.innovation.takatak                                          |4          |28606            |
# |com.next.innovation.takatak                                          |0          |27144            |
# |in.mohalla.sharechat                                                 |7          |26303            |
# |com.next.innovation.takatak                                          |5          |24076            |
# |in.mohalla.video                                                     |7          |22517            |
# |in.mohalla.sharechat                                                 |8          |20585            |
# |in.mohalla.sharechat                                                 |20         |19971            |
# |com.next.innovation.takatak                                          |6          |19508            |
# |in.mohalla.video                                                     |8          |16454            |
# |in.mohalla.sharechat                                                 |9          |15328            |
# |com.next.innovation.takatak                                          |7          |15298            |
# |in.mohalla.video                                                     |9          |11494            |
# |in.mohalla.sharechat                                                 |10         |11241            |
# |com.next.innovation.takatak                                          |8          |10924            |
# |com.next.innovation.takatak                                          |20         |8936             |
# |in.mohalla.video                                                     |10         |7745             |
# |in.mohalla.sharechat                                                 |11         |7694             |
# |com.next.innovation.takatak                                          |9          |7306             |
# |in.mohalla.sharechat                                                 |12         |5815             |
# |in.mohalla.video                                                     |11         |5699             |
# |in.mohalla.video                                                     |12         |4599             |
# |in.mohalla.sharechat                                                 |13         |4528             |
# |com.next.innovation.takatak                                          |10         |4448             |
# |in.mohalla.video                                                     |13         |3957             |
# |in.mohalla.sharechat                                                 |14         |3691             |
# |in.mohalla.video                                                     |14         |3335             |
# |in.mohalla.sharechat                                                 |15         |2981             |
# |in.mohalla.video                                                     |15         |2966             |
# |com.next.innovation.takatak                                          |11         |2837             |
# |in.mohalla.video                                                     |16         |2629             |
# |in.mohalla.sharechat                                                 |16         |2514             |
# |in.mohalla.video                                                     |17         |2481             |
# |video.like                                                           |0          |2382             |
# |in.mohalla.sharechat                                                 |17         |2270             |
# |in.mohalla.video                                                     |18         |2249             |
# |com.next.innovation.takatak                                          |12         |2130             |
# |in.mohalla.video                                                     |19         |1981             |
# |com.vivashow.share.video.chat                                        |0          |1974             |
# |com.vivashow.share.video.chat                                        |1          |1859             |
# |in.mohalla.sharechat                                                 |18         |1840             |
# |instagram.video.downloader.story.saver                               |0          |1746             |
# |in.mohalla.sharechat                                                 |19         |1710             |
# |com.next.innovation.takatak                                          |13         |1678             |
# |com.vivashow.share.video.chat                                        |2          |1616             |
# |instagram.video.downloader.story.saver                               |1          |1552             |
# |instagram.video.downloader.story.saver                               |2          |1512             |
# |com.eterno.shortvideos                                               |1          |1508             |
# |com.eterno.shortvideos                                               |0          |1450             |
# |com.next.innovation.takatak                                          |14         |1438             |
# |in.mohalla.video.lite                                                |0          |1432             |
# |com.vivashow.share.video.chat                                        |3          |1403             |
# |instagram.video.downloader.story.saver                               |3          |1331             |
# |com.eterno.shortvideos                                               |2          |1253             |
# |com.vivashow.share.video.chat                                        |4          |1189             |
# |com.next.innovation.takatak                                          |15         |1186             |
# |instagram.video.downloader.story.saver                               |4          |1136             |
# |com.next.innovation.takatak                                          |16         |1106             |
# |instagram.video.downloader.story.saver                               |5          |990              |
# |com.vivashow.share.video.chat                                        |5          |982              |
# |com.eterno.shortvideos                                               |3          |972              |
# |com.next.innovation.takatak                                          |17         |947              |
# |com.lazygeniouz.saveit                                               |2          |928              |
# |com.lazygeniouz.saveit                                               |3          |867              |
# |instagram.video.downloader.story.saver                               |6          |841              |
# |com.mosin.lee.gbversion.latest                                       |0          |835              |
# |com.next.innovation.takatak                                          |18         |806              |
# |com.kkmversion.thelightv                                             |0          |772              |
# |com.lazygeniouz.saveit                                               |4          |771              |
# |sun.way2sms.hyd.com                                                  |2          |770              |
# |com.lazygeniouz.saveit                                               |1          |764              |
# |com.vivashow.share.video.chat                                        |6          |760              |
# |get.lokal.localnews                                                  |2          |755              |
# |sun.way2sms.hyd.com                                                  |3          |745              |
# |com.eterno.shortvideos                                               |4          |741              |
# |com.next.innovation.takatak                                          |19         |736              |
# |get.lokal.localnews                                                  |3          |691              |
# |sun.way2sms.hyd.com                                                  |4          |690              |
# |get.lokal.localnews                                                  |1          |668              |
# |com.eterno.shortvideos                                               |20         |658              |
# |com.vivashow.share.video.chat                                        |7          |653              |
# |instagram.video.downloader.story.saver                               |20         |648              |
# |com.lazygeniouz.saveit                                               |0          |633              |
# |get.lokal.localnews                                                  |4          |623              |
# |instagram.video.downloader.story.saver                               |7          |618              |
# |sun.way2sms.hyd.com                                                  |1          |616              |
# |sun.way2sms.hyd.com                                                  |5          |555              |
# |com.lazygeniouz.saveit                                               |5          |553              |
# |instagram.video.downloader.story.saver                               |8          |549              |
# |com.lazygeniouz.saveit                                               |6          |538              |
# |com.eterno.shortvideos                                               |5          |521              |
# |get.lokal.localnews                                                  |0          |519              |
# |com.vivashow.share.video.chat                                        |20         |497              |
# |io.chingari.app                                                      |0          |487              |
# |sun.way2sms.hyd.com                                                  |0          |486              |
# |get.lokal.localnews                                                  |5          |480              |
# |get.lokal.localnews                                                  |20         |471              |
# |sun.way2sms.hyd.com                                                  |6          |468              |
# |com.blockchainvault                                                  |3          |451              |
# |com.vivashow.share.video.chat                                        |8          |451              |
# |com.eterno.shortvideos                                               |6          |449              |
# |io.chingari.app                                                      |1          |439              |
# |com.blockchainvault                                                  |4          |433              |
# |com.dianyun.chikii                                                   |20         |429              |
# |com.blockchainvault                                                  |20         |427              |
# |get.lokal.localnews                                                  |6          |427              |
# |com.blockchainvault                                                  |5          |408              |
# |com.blockchainvault                                                  |2          |406              |
# |instagram.video.downloader.story.saver                               |9          |404              |
# |com.lazygeniouz.saveit                                               |7          |398              |
# |video.like.lite                                                      |0          |395              |
# |com.vivashow.share.video.chat                                        |9          |389              |
# |in.mohalla.video.lite                                                |1          |388              |
# |com.mosin.lee.gbversion.latest                                       |1          |383              |
# |sun.way2sms.hyd.com                                                  |7          |376              |
# |com.eterno.shortvideos                                               |7          |372              |
# |io.chingari.app                                                      |2          |365              |
# |com.kkmversion.thelightv                                             |1          |353              |
# |instagram.video.downloader.story.saver                               |10         |349              |
# |com.lazygeniouz.saveit                                               |8          |339              |
# |com.blockchainvault                                                  |1          |322              |
# |com.lazygeniouz.saveit                                               |20         |320              |
# |com.rasilo.gbapk.latestversion.gbdownload                            |0          |320              |
# |xdt.statussaver.downloadstatus.savestatus                            |0          |318              |
# |com.blockchainvault                                                  |7          |313              |
# |com.blockchainvault                                                  |6          |311              |
# |get.lokal.localnews                                                  |7          |310              |
# |get.lokal.localnews                                                  |8          |296              |
# |com.mosin.lee.gbversion.latest                                       |2          |295              |
# |messenger.video.call.chat.free                                       |0          |292              |
# |com.vivashow.share.video.chat                                        |10         |291              |
# |sun.way2sms.hyd.com                                                  |8          |286              |
# |messenger.social.chat.apps                                           |0          |282              |
# |io.chingari.app                                                      |3          |281              |
# |com.ck                                                               |20         |274              |
# |com.eterno.shortvideos                                               |8          |270              |
# |com.blockchainvault                                                  |0          |258              |
# |photo.video.instasaveapp                                             |0          |257              |
# |sun.way2sms.hyd.com                                                  |20         |252              |
# |com.blockchainvault                                                  |8          |250              |
# |com.lazygeniouz.saveit                                               |9          |250              |
# |instagram.video.downloader.story.saver                               |11         |248              |
# |com.blockchainvault                                                  |9          |243              |
# |io.chingari.app                                                      |4          |243              |
# |com.kkmversion.thelightv                                             |2          |234              |
# |com.thinkmobile.accountmaster                                        |0          |227              |
# |com.eterno.shortvideos                                               |9          |222              |
# |com.mosin.lee.gbversion.latest                                       |3          |221              |
# |video.like                                                           |1          |214              |
# |sun.way2sms.hyd.com                                                  |9          |213              |
# |com.bela.live                                                        |20         |212              |
# |com.kkmversion.thelightv                                             |3          |212              |
# |com.vivashow.share.video.chat                                        |11         |203              |
# |instagram.video.downloader.story.saver                               |12         |203              |
# |get.lokal.localnews                                                  |9          |200              |
# |messenger.chat.social.messenger                                      |0          |195              |
# |com.lazygeniouz.saveit                                               |10         |191              |
# |com.roposo.android                                                   |0          |191              |
# |com.eterno.shortvideos                                               |10         |189              |
# |com.blockchainvault                                                  |10         |186              |
# |gbstorysaver.gbtoolkit.gbwhatdownload.latestgb                       |20         |181              |
# |io.chingari.app                                                      |5          |179              |
# |com.facechat.live                                                    |2          |177              |
# |com.magic.whatsapp.status.saver.download                             |0          |176              |
# |com.facechat.live                                                    |5          |175              |
# |com.kkmversion.thelightv                                             |5          |171              |
# |com.mosin.lee.gbversion.latest                                       |4          |169              |
# |com.facechat.live                                                    |3          |167              |
# |com.kkmversion.thelightv                                             |4          |166              |
# |com.facechat.live                                                    |4          |165              |
# |com.roposo.android                                                   |1          |165              |
# |photo.video.instasaveapp                                             |1          |164              |
# |com.grindrapp.android                                                |3          |163              |
# |in.mohalla.video.lite                                                |2          |163              |
# |instagram.video.downloader.story.saver                               |13         |163              |
# |videomaker.slideshow.editor.photo.editor                             |0          |163              |
# |com.facechat.live                                                    |20         |162              |
# |com.grindrapp.android                                                |2          |162              |
# |com.grindrapp.android                                                |4          |162              |
# |sun.way2sms.hyd.com                                                  |10         |159              |
# |com.magic.whatsapp.status.saver.download                             |2          |158              |
# |com.khushwant.sikhworld                                              |1          |155              |
# |io.chingari.app                                                      |20         |153              |
# |com.bela.live                                                        |3          |149              |
# |com.mosin.lee.gbversion.latest                                       |20         |147              |
# |com.khushwant.sikhworld                                              |0          |146              |
# |com.bela.live                                                        |5          |145              |
# |com.facechat.live                                                    |1          |144              |
# |com.grindrapp.android                                                |5          |143              |
# |com.kkmversion.thelightv                                             |20         |143              |
# |com.stackwares.android.storysaver.facebook                           |0          |140              |
# |io.chingari.app                                                      |6          |140              |
# |com.magic.whatsapp.status.saver.download                             |1          |138              |
# |com.vivashow.share.video.chat                                        |12         |138              |
# |get.lokal.localnews                                                  |10         |138              |
# |com.bela.live                                                        |4          |136              |
# |com.eterno.shortvideos                                               |11         |136              |
# |com.kkmversion.thelightv                                             |6          |136              |
# |instagram.storysaver.story.downloader.videodownloader.reelsdownloader|0          |136              |
# |photo.video.instasaveapp                                             |2          |136              |
# |com.bela.live                                                        |2          |135              |
# |com.mogul.flutte                                                     |20         |135              |
# |com.bunny                                                            |20         |134              |
# |com.facechat.live                                                    |6          |134              |
# |com.lazygeniouz.saveit                                               |11         |134              |
# |com.stackwares.android.storysaver.facebook                           |1          |134              |
# |com.mosin.lee.gbversion.latest                                       |5          |133              |
# |com.vivashow.share.video.chat                                        |13         |132              |
# |com.grindrapp.android                                                |1          |131              |
# |xdt.statussaver.downloadstatus.savestatus                            |1          |131              |
# |com.khushwant.sikhworld                                              |2          |130              |
# |com.magic.whatsapp.status.saver.download                             |3          |130              |
# |nikhil.nixify.satbarautaramaharashtra                                |0          |130              |
# |instagram.video.downloader.story.saver                               |14         |129              |
# |com.grindrapp.android                                                |0          |128              |
# |com.rasilo.gbapk.latestversion.gbdownload                            |1          |128              |
# |me.dingtone.app.im                                                   |0          |126              |
# |com.facechat.live                                                    |7          |125              |
# |com.grindrapp.android                                                |6          |124              |
# |com.blued.international                                              |3          |123              |
# |com.eterno.shortvideos                                               |12         |123              |
# |com.gbwp.latestgbwp.statussaver                                      |0          |122              |
# |com.eterno.shortvideos                                               |13         |120              |
# |photo.video.instasaveapp                                             |3          |119              |
# |com.blued.international                                              |2          |118              |
# |io.chingari.app                                                      |7          |118              |
# |instagram.video.downloader.story.saver                               |15         |117              |
# |com.mosin.lee.gbversion.latest                                       |6          |116              |
# |com.grindrapp.android                                                |7          |114              |
# |com.bela.live                                                        |7          |111              |
# |sun.way2sms.hyd.com                                                  |11         |109              |
# |com.blockchainvault                                                  |11         |108              |
# |com.blued.international                                              |5          |107              |
# |com.thinkmobile.accountmaster                                        |1          |107              |
# |instagram.video.downloader.story.saver                               |16         |107              |
# |com.bela.live                                                        |6          |104              |
# |gbstorysaver.gbtoolkit.gbwhatdownload.latestgb                       |4          |104              |
# |get.lokal.localnews                                                  |11         |104              |
# |com.vivashow.share.video.chat                                        |14         |102              |
# |gbstorysaver.gbtoolkit.gbwhatdownload.latestgb                       |5          |102              |
# |videomaker.slideshow.editor.photo.editor                             |1          |102              |
# |com.eterno.shortvideos                                               |14         |101              |
# |com.blued.international                                              |4          |100              |
# |com.blued.international                                              |6          |99               |
# |com.download.whatstatus                                              |0          |99               |
# |com.kkmversion.thelightv                                             |8          |99               |
# |com.stackwares.android.storysaver.facebook                           |2          |99               |
# |com.facechat.live                                                    |0          |98               |
# |com.facechat.live                                                    |10         |98               |
# |com.facechat.live                                                    |8          |98               |
# |com.oasis.world                                                      |20         |98               |
# |videomaker.slideshow.editor.photo.editor                             |2          |98               |
# |com.bela.live                                                        |1          |97               |
# |com.blued.international                                              |1          |97               |
# |com.facechat.live                                                    |9          |97               |
# |com.khushwant.sikhworld                                              |3          |96               |
# |com.kkmversion.thelightv                                             |7          |96               |
# |com.magic.whatsapp.status.saver.download                             |4          |94               |
# |com.stackwares.android.storysaver.facebook                           |3          |94               |
# |multipleaccounts.parallelspace.cloneapp.appclonerstudio              |0          |94               |
# |com.bela.live                                                        |9          |93               |
# |instagram.video.downloader.story.saver                               |17         |93               |
# |com.vivashow.share.video.chat                                        |15         |92               |
# |xdt.statussaver.downloadstatus.savestatus                            |2          |92               |
# |com.blued.international                                              |0          |91               |
# |com.exutech.chacha                                                   |2          |91               |
# |instagram.storysaver.story.downloader.videodownloader.reelsdownloader|1          |91               |
# |instore.tools.app                                                    |0          |91               |
# |messenger.video.call.chat.free                                       |1          |90               |
# |com.videoat.downvidup                                                |0          |89               |
# |playit.videoplayer.musicplayer                                       |0          |89               |
# |com.exutech.chacha                                                   |1          |88               |
# |com.roposo.android                                                   |2          |88               |
# |com.blockchainvault                                                  |12         |87               |
# |com.grindrapp.android                                                |8          |87               |
# |io.chingari.app                                                      |8          |87               |
# |photo.video.instasaveapp                                             |4          |87               |
# |com.bela.live                                                        |0          |86               |
# |com.lazygeniouz.saveit                                               |12         |86               |
# |gbstorysaver.gbtoolkit.gbwhatdownload.latestgb                       |7          |86               |
# |in.mohalla.video.lite                                                |3          |86               |
# |instagram.storysaver.story.downloader.videodownloader.reelsdownloader|2          |86               |
# |messenger.social.chat.apps                                           |1          |86               |
# |com.bunny                                                            |4          |85               |
# |com.download.whatstatus                                              |2          |85               |
# |com.stackwares.android.storysaver.facebook                           |4          |85               |
# |com.bela.live                                                        |8          |84               |
# |com.mosin.lee.gbversion.latest                                       |7          |84               |
# |com.status.downloader.video.image.saver                              |0          |84               |
# |gbstorysaver.gbtoolkit.gbwhatdownload.latestgb                       |6          |84               |
# |com.download.whatstatus                                              |1          |83               |
# |com.grindrapp.android                                                |9          |83               |
# |com.khushwant.sikhworld                                              |4          |83               |
# |gbstorysaver.gbtoolkit.gbwhatdownload.latestgb                       |8          |83               |
# |com.exutech.chacha                                                   |3          |82               |
# |gbstorysaver.gbtoolkit.gbwhatdownload.latestgb                       |3          |82               |
# |com.bunny                                                            |2          |81               |
# |instore.tools.app                                                    |1          |81               |
# |xdt.statussaver.downloadstatus.savestatus                            |3          |81               |
# |com.rasilo.gbapk.latestversion.gbdownload                            |2          |80               |
# |get.lokal.localnews                                                  |12         |80               |
# |videodownloader.statussaver.statusdownloader                         |0          |80               |
# |me.dingtone.app.im                                                   |1          |79               |
# |com.exutech.chacha                                                   |4          |78               |
# |com.whazappstatus.whasappstatus                                      |0          |78               |
# |video.like                                                           |2          |78               |
# |com.bunny                                                            |3          |77               |
# |com.kkmversion.thelightv                                             |9          |77               |
# |sun.way2sms.hyd.com                                                  |12         |77               |
# |taptap.twoaccounts.dual.space                                        |0          |77               |
# |com.dianyun.chikii                                                   |0          |76               |
# |com.vivashow.share.video.chat                                        |16         |76               |
# |com.zhiliaoapp.musically                                             |0          |76               |
# |com.dianyun.chikii                                                   |4          |75               |
# |com.dianyun.chikii                                                   |6          |75               |
# |com.exutech.chacha                                                   |0          |75               |
# |com.thinkmobile.accountmaster                                        |2          |75               |
# |videomaker.slideshow.editor.photo.editor                             |4          |75               |
# |gbstorysaver.gbtoolkit.gbwhatdownload.latestgb                       |2          |74               |
# |gbstorysaver.gbtoolkit.gbwhatdownload.latestgb                       |9          |74               |
# |messenger.chat.social.messenger                                      |1          |74               |
# |com.bunny.lite                                                       |4          |73               |
# |com.dianyun.chikii                                                   |3          |73               |
# |com.lazygeniouz.saveit                                               |13         |73               |
# |com.magic.whatsapp.status.saver.download                             |5          |73               |
# |instagram.video.downloader.story.saver                               |18         |73               |
# |com.dianyun.chikii                                                   |1          |72               |
# |com.mosin.lee.gbversion.latest                                       |8          |72               |
# |get.lokal.localnews                                                  |13         |72               |
# |com.andromo.dev682265.app1072421                                     |0          |71               |
# |com.bunny                                                            |5          |71               |
# |com.dianyun.chikii                                                   |2          |70               |
# |com.bunny                                                            |1          |69               |
# |com.dianyun.chikii                                                   |9          |69               |
# |com.mosin.lee.gbversion.latest                                       |9          |69               |
# |photo.video.instasaveapp                                             |5          |69               |
# |videomaker.slideshow.editor.photo.editor                             |3          |69               |
# |com.bunny.lite                                                       |2          |68               |
# |com.facechat.live                                                    |12         |68               |
# |com.grindrapp.android                                                |20         |68               |
# |messenger.video.call.chat.free                                       |2          |68               |
# |com.blued.international                                              |7          |67               |
# |com.bunny                                                            |6          |67               |
# |com.dianyun.chikii                                                   |7          |67               |
# |gbstorysaver.gbtoolkit.gbwhatdownload.latestgb                       |10         |67               |
# |com.eterno.shortvideos                                               |19         |66               |
# |com.facechat.live                                                    |11         |66               |
# |com.gbpro.whtsappna                                                  |0          |66               |
# |com.rasilo.gbapk.latestversion.gbdownload                            |3          |66               |
# |com.eterno.shortvideos                                               |17         |64               |
# |com.exutech.chacha                                                   |20         |64               |
# |com.mosin.lee.gbversion.latest                                       |10         |64               |
# |com.thinkmobile.accountmaster                                        |3          |64               |
# |me.dingtone.app.im                                                   |3          |64               |
# |com.bunny.lite                                                       |1          |63               |
# |com.bunny.lite                                                       |20         |63               |
# |com.dianyun.chikii                                                   |5          |63               |
# |com.download.whatstatus                                              |3          |63               |
# |com.stackwares.android.storysaver.facebook                           |6          |63               |
# |io.chingari.app                                                      |9          |63               |
# |com.eterno.shortvideos                                               |15         |62               |
# |com.bela.live                                                        |12         |61               |
# |com.facechat.live                                                    |14         |61               |
# |com.magic.whatsapp.status.saver.download                             |6          |61               |
# |com.stackwares.android.storysaver.facebook                           |5          |61               |
# |instagram.storysaver.story.downloader.videodownloader.reelsdownloader|3          |61               |
# |instore.tools.app                                                    |2          |61               |
# |me.dingtone.app.im                                                   |2          |61               |
# |messenger.chat.social.messenger                                      |2          |61               |
# |com.bela.live                                                        |10         |60               |
# |com.bela.live                                                        |11         |60               |
# |com.blockchainvault                                                  |15         |60               |
# |com.kkmversion.thelightv                                             |10         |60               |
# |com.lazygeniouz.saveit                                               |14         |60               |
# |com.mosin.lee.gbversion.latest                                       |11         |60               |
# |com.waf.lovemessageforgf                                             |0          |60               |
# |get.lokal.localnews                                                  |15         |60               |
# |photo.video.instasaveapp                                             |6          |60               |
# |save.videos.officially                                               |20         |60               |
# |com.bunny.lite                                                       |3          |59               |
# |com.facechat.live                                                    |13         |59               |
# |com.music.gbwhatsapp.gblatestversion                                 |0          |59               |
# |com.videoat.downvidup                                                |1          |59               |
# |spiritualstudio.hanumanchalisa                                       |0          |59               |
# |statussaver.deleted.messages.savevidieos                             |0          |59               |
# |videomaker.slideshow.editor.photo.editor                             |5          |59               |
# |com.eterno.shortvideos                                               |16         |58               |
# |me.dingtone.app.im                                                   |5          |58               |
# |com.dianyun.chikii                                                   |11         |57               |
# |com.messenger.messengerpro.social.chat                               |0          |57               |
# |me.dingtone.app.im                                                   |20         |57               |
# |com.blockchainvault                                                  |13         |56               |
# |com.blockchainvault                                                  |14         |56               |
# |com.magic.whatsapp.status.saver.download                             |20         |56               |
# |instagram.video.downloader.story.saver                               |19         |56               |
# |sun.way2sms.hyd.com                                                  |14         |56               |
# |com.exutech.chacha                                                   |5          |55               |
# |com.vivashow.share.video.chat                                        |17         |55               |
# |get.lokal.localnews                                                  |14         |55               |
# |com.khushwant.sikhworld                                              |5          |54               |
# |com.vivashow.share.video.chat                                        |18         |54               |
# |sun.way2sms.hyd.com                                                  |13         |54               |
# |xdt.statussaver.downloadstatus.savestatus                            |4          |54               |
# |com.bunny                                                            |9          |53               |
# |com.gbwp.latestgbwp.statussaver                                      |1          |53               |
# |com.magic.whatsapp.status.saver.download                             |7          |53               |
# |com.status.downloader.video.image.saver                              |1          |53               |
# |com.whazappstatus.whasappstatus                                      |1          |53               |
# |gbstorysaver.gbtoolkit.gbwhatdownload.latestgb                       |1          |53               |
# |get.lokal.localnews                                                  |16         |53               |
# |instagram.storysaver.story.downloader.videodownloader.reelsdownloader|4          |53               |
# |io.chingari.app                                                      |10         |53               |
# |photo.video.instasaveapp                                             |7          |53               |
# |statussaver.statusdownloader.downloadstatus.videoimagesaver          |0          |53               |
# |com.blockchainvault                                                  |17         |52               |
# |com.blued.international                                              |8          |52               |
# |com.bunny                                                            |0          |52               |
# |com.download.whatstatus                                              |4          |52               |
# |com.exutech.chacha                                                   |6          |52               |
# |com.gagalite.live                                                    |20         |52               |
# |gplayzones.purushottam.oneapp                                        |0          |52               |
# |com.dianyun.chikii                                                   |12         |51               |
# |com.domobile.messenger                                               |1          |51               |
# |videomaker.slideshow.editor.photo.editor                             |20         |51               |
# |xdt.statussaver.downloadstatus.savestatus                            |5          |51               |
# |com.belalite.live                                                    |2          |50               |
# |com.lazygeniouz.saveit                                               |15         |50               |
# |gbstorysaver.gbtoolkit.gbwhatdownload.latestgb                       |0          |50               |
# |gbstorysaver.gbtoolkit.gbwhatdownload.latestgb                       |11         |50               |
# |in.mohalla.video.lite                                                |4          |50               |
# |com.bunny.lite                                                       |5          |49               |
# |com.promo.ema.followersfinder                                        |0          |49               |
# |com.belalite.live                                                    |1          |48               |
# |com.belalite.live                                                    |3          |48               |
# |com.peach.live                                                       |0          |48               |
# |com.roposo.android                                                   |3          |48               |
# |get.lokal.localnews                                                  |17         |48               |
# |video.like                                                           |3          |48               |
# |video.like.lite                                                      |1          |48               |
# |com.blockchainvault                                                  |18         |47               |
# |com.magic.whatsapp.status.saver.download                             |8          |47               |
# |com.peach.live                                                       |1          |47               |
# |me.dingtone.app.im                                                   |4          |47               |
# |messenger.chat.social.messenger                                      |3          |47               |
# |com.blued.international                                              |9          |46               |
# |com.bunny.lite                                                       |6          |46               |
# |com.grindrapp.android                                                |10         |46               |
# |com.hidespps.apphider                                                |20         |46               |
# |com.meeta.live                                                       |20         |46               |
# |com.peach.live                                                       |2          |46               |
# |com.yy.hiyo                                                          |0          |46               |
# |instore.tools.app                                                    |3          |46               |
# |me.dingtone.app.im                                                   |6          |46               |
# |photo.video.instasaveapp                                             |20         |46               |
# |statussaver.gbwhatslatestversion.statusdownloader                    |0          |46               |
# |com.andromo.dev682265.app1072421                                     |1          |45               |
# |com.dianyun.chikii                                                   |8          |45               |
# |com.eterno.shortvideos                                               |18         |45               |
# |com.exutech.chacha                                                   |7          |45               |
# |com.ratelekom.findnow                                                |4          |45               |
# |com.stackwares.android.storysaver.facebook                           |7          |45               |
# |com.thinkmobile.accountmaster                                        |4          |45               |
# |com.bunny                                                            |7          |44               |
# |com.ratelekom.findnow                                                |20         |44               |
# |messenger.video.call.chat.free                                       |3          |44               |
# |sun.way2sms.hyd.com                                                  |15         |44               |
# |com.blued.international                                              |20         |43               |
# |com.dianyun.chikii                                                   |10         |43               |
# |com.domobile.messenger                                               |0          |43               |
# |com.khushwant.sikhworld                                              |6          |43               |
# |com.ratelekom.findnow                                                |2          |43               |
# |com.waf.lovemessageforgf                                             |1          |43               |
# |io.chingari.app                                                      |11         |43               |
# |messenger.video.call.chat.free                                       |20         |43               |
# |com.kkmversion.thelightv                                             |13         |42               |
# |com.odass.status.saver                                               |1          |42               |
# |com.stackwares.android.storysaver.facebook                           |20         |42               |
# |com.thinkmobile.accountmaster                                        |20         |42               |
# |com.vivashow.share.video.chat                                        |19         |42               |
# |com.bela.live                                                        |13         |41               |
# |com.evecompany.statusversion                                         |0          |41               |
# |com.scorp.who                                                        |0          |41               |
# |com.waf.lovemessageforbf                                             |0          |41               |
# +---------------------------------------------------------------------+-----------+-----------------+


#                                                bundle  index_label_count
# 51                               in.mohalla.sharechat             466712
# 52                                   in.mohalla.video             448596
# 29                        com.next.innovation.takatak             267356
# 42                      com.vivashow.share.video.chat              12958
# 55             instagram.video.downloader.story.saver              12865
# 11                             com.eterno.shortvideos               9380
# 22                             com.lazygeniouz.saveit               6955
# 49                                get.lokal.localnews               6050
# 71                                sun.way2sms.hyd.com               5956
# 3                                 com.blockchainvault               4474
# 73                                         video.like               2722
# 57                                    io.chingari.app               2651
# 27                     com.mosin.lee.gbversion.latest               2648


# 分析安装列表的长度
    spark.sql(
    """
    select 
        len_label,count(device_id) as device_cnt
    from 
    (
        select 
            device_id,
            if(size(user_bundle_list)<20,size(user_bundle_list),20) as len_label
        from 
            positive_data_table
    ) t1 
    group by 
        len_label
    order by 
        device_cnt desc,len_label
    """
).show(200,truncate=False)
# +---------+----------+
# |len_label|device_cnt|
# +---------+----------+
# |20       |444468    |
# |5        |280485    |
# |6        |274227    |
# |7        |263544    |
# |8        |249078    |
# |4        |246461    |
# |9        |234926    |
# |10       |220027    |
# |11       |204074    |
# |3        |172261    |
# |12       |151378    |
# |13       |115548    |
# |14       |91939     |
# |15       |74705     |
# |16       |62288     |
# |17       |52999     |
# |18       |44860     |
# |19       |39397     |
# +---------+----------+

if __name__ == '__main__':
    spark = SparkSession.builder.appName("cr_ana").getOrCreate()
