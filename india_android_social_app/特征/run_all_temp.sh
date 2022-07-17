set -e
date_now=`date +%Y%m%d`
echo "date_now:${date_now}"
model_day=`date -d "${date_now} -5 day" +%Y%m%d`
echo "model_day ${model_day}"
predict_day=`date -d"${date_now} -1 day" +%Y%m%d`
echo "predict_day ${predict_day}"
model_test_day=`date -d "${date_now} -4 day" +%Y%m%d`
echo "model_test_day ${model_test_day}"
## 构建人群库
#spark-submit \
#--deploy-mode client --master yarn  \
#--driver-memory 10G --executor-memory 55G --num-executors 10 --executor-cores 32 \
#--conf spark.driver.memoryOverhead=1024 --conf spark.executor.memoryOverhead=1024 --conf spark.sql.shuffle.partitions=10000 --conf spark.yarn.maxAppAttempts=1  oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/create_candidate_data.py  --sample_date ${predict_day} > ${predict_day}_candidate.log 2>&1
#hdfs dfs -put ${predict_day}_candidate.log oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/${predict_day}_candidate.log

# 构建样本
spark-submit \
--deploy-mode client --master yarn  \
--driver-memory 10G --executor-memory 50G --num-executors 10 --executor-cores 32 \
--conf spark.driver.memoryOverhead=1024 --conf spark.executor.memoryOverhead=1024 --conf spark.sql.shuffle.partitions=10000 --conf spark.yarn.maxAppAttempts=1  oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/create_social_ana_sample_from_oss_delay.py  --sample_date ${model_test_day} > ${model_test_day}_sample.log 2>&1
hdfs dfs -put ${model_test_day}_sample.log oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/${model_test_day}_sample.log

# 训练模型
spark-submit \
--deploy-mode cluster \
--master yarn    \
--driver-memory 10G --executor-memory 50G --num-executors 10 --executor-cores 32 \
--conf spark.driver.memoryOverhead=1024  \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.sql.shuffle.partitions=10000 \
--conf spark.yarn.maxAppAttempts=1  \
--archives=oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/py370.zip#PY3  \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
--conf spark.pyspark.python=./PY3/py370/bin/python3 \
oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/pyspark_gbdt.py --model_day ${model_day} --model_index 4 --test_day_sure ${model_test_day}  --candidate_day ${predict_day} --candidate_predict_index 0

