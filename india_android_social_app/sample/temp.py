
oss_dirPath='oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app'
sample_date='20220617'
df_neg_social = spark.read.format('orc').load("%s/oss_%s_neg_sample_social_delay" % (oss_dirPath, sample_date))
df_neg_social.createTempView("df_table_neg_social_2")
df_neg = spark.read.format('orc').load("%s/oss_%s_neg_sample_delay_2" % (oss_dirPath, sample_date))
df_neg.createTempView("df_table_neg_2")
spark.sql(
    """
    select
        t1.device_id 
        ,bundle_social_set
        ,bundle_social_set_size
        ,bundle_social_set_2
        ,bundle_social_set_2_size
    from 
        df_table_neg t1
    inner join 
        df_table_neg_social t2
    on 
        t1.device_id = t2.device_id 
    where
        bundle_social_set_size < bundle_social_set_2_size
    """
).show(50)

