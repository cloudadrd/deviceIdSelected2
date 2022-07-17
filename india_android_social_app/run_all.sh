date_now=`date +%Y%m%d`
echo "date_now:${date_now}"
# 目前模型没有每天更新，model_day 未生效
model_day=`date -d "${date_now} -4 day" +%Y%m%d`
echo "model_day ${model_day}"
predict_day=`date -d"${date_now} -1 day" +%Y%m%d`
echo "predict_day ${predict_day}"
model_test_day=`date -d "${date_now} -3 day" +%Y%m%d`
echo "model_test_day ${model_test_day}"
driver_memory='10G'
executor_memory='25G'
num_executors=10
executor_cores=4

user_data_dir="oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=${predict_day}/_SUCCESS"
hdfs  dfs  -find ${user_data_dir}
if [ $? -ne 0 ]; then
    echo "${user_data_dir} not exists!"
    exit $?
else
    echo "${user_data_dir}  exists!"
fi

set -e
echo "构建人群库"
#spark-submit \
#--deploy-mode client --master yarn  \
#--driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
#--conf spark.driver.memoryOverhead=1024 --conf spark.executor.memoryOverhead=1024 --conf spark.sql.shuffle.partitions=10000 --conf spark.yarn.maxAppAttempts=1  oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/create_candidate_data.py  --sample_date ${predict_day} > ${predict_day}_candidate.log 2>&1
#hdfs dfs -put ${predict_day}_candidate.log oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/${predict_day}_candidate.log

echo "构建样本"
spark-submit \
--deploy-mode client --master yarn  \
--driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
--conf spark.driver.memoryOverhead=1024 --conf spark.executor.memoryOverhead=1024 --conf spark.sql.shuffle.partitions=10000 --conf spark.yarn.maxAppAttempts=1  oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/create_social_ana_sample_from_oss_delay.py  --sample_date ${model_test_day} > ${model_test_day}_sample.log 2>&1
hdfs dfs -put ${model_test_day}_sample.log oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/${model_test_day}_sample.log

echo "训练模型"
#spark-submit \
#--deploy-mode cluster \
#--master yarn    \
#--driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
#--conf spark.driver.memoryOverhead=1024  \
#--conf spark.executor.memoryOverhead=1024 \
#--conf spark.sql.shuffle.partitions=10000 \
#--conf spark.yarn.maxAppAttempts=1  \
#--archives=oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/py370.zip#PY3  \
#--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
#--conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
#--conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
#--conf spark.pyspark.python=./PY3/py370/bin/python3 \
#oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/pyspark_gbdt.py \
#--model_day '20220623' \
#--model_index 0 \
#--maxIter 35 \
#--maxDepth  15 \
#--minInfoGain 0 \
#--minInstancesPerNode 200 \
#--candidate_day  ${predict_day} \
#--candidate_predict_index 0 \
#--test_day_sure ${model_test_day}

# 将oss 数据保存至 s3
#rclone sync  oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/oss_20220627_candidate_predict_0_csv/ s3://emr-gift-sin-bj/model/social_app_predict/oss_20220627_candidate_predict_0_csv/  --progress --s3-chunk-size 64M --s3-upload-concurrency 16

