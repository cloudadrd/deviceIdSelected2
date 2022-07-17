sample_date_2='20220622'
spark-submit \
--deploy-mode client --master yarn  \
--driver-memory 30G --executor-memory 50G --num-executors 10 --executor-cores 16 \
--conf spark.driver.memoryOverhead=1024 --conf spark.executor.memoryOverhead=1024 --conf spark.sql.shuffle.partitions=10000 --conf spark.yarn.maxAppAttempts=1  oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/create_social_ana_sample_from_oss_delay.py  --sample_date $sample_date_2 > $sample_date_2.log 2>&1
hdfs dfs -put $sample_date_2.log oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/$sample_date_2.log