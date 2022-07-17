#date_now=`date +%Y%m%d`
date_now='20220630'
echo "date_now:${date_now}"
# 目前模型没有每天更新，model_day 未生效
model_day=`date -d "${date_now} -6 day" +%Y%m%d`
echo "model_day ${model_day}"
predict_day=`date -d"${date_now} -1 day" +%Y%m%d`
echo "predict_day ${predict_day}"
model_test_day=`date -d "${date_now} -5 day" +%Y%m%d`
echo "model_test_day ${model_test_day}"
driver_memory='30G'
executor_memory='53G'
num_executors=10
executor_cores=16

#  --conf spark.driver.memoryOverhead=1024  \
#  --conf spark.executor.memoryOverhead=1024 \

index=7

user_data_dir="oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=${predict_day}/_SUCCESS"
is_exist=0
while [ ${is_exist} -eq 0 ]
do
    hdfs  dfs  -find ${user_data_dir}
    if [ $? -ne 0 ]; then
        echo "${user_data_dir} not exists!"
        sleep 5m
    else
        echo "${user_data_dir}  exists!"
        is_exist=1
    fi
done


set -e

echo "构建人群库 ${predict_day}"
spark-submit --deploy-mode client --master yarn  --driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} --conf spark.sql.shuffle.partitions=500 --conf spark.yarn.maxAppAttempts=1  oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/create_candidate_data.py  --sample_date ${predict_day} --lastActivedays ${index} > ${predict_day}_candidate_${index}.log 2>&1
hdfs dfs -put ${predict_day}_candidate_7.log oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/${predict_day}_candidate_${index}.log


echo "构建样本 ${model_test_day}"
spark-submit --deploy-mode client --master yarn  --driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} --conf spark.sql.shuffle.partitions=500  --conf spark.yarn.maxAppAttempts=1  oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/create_social_ana_sample_from_oss_delay.py  --sample_date ${model_test_day} --lastActivedays ${index} > ${model_test_day}_sample_${index}.log 2>&1
hdfs dfs -put ${model_test_day}_sample_${index}.log oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/${model_test_day}_sample_${index}.log

#每周一更新一次模型
weekday=`date -d "${date_now}" +%u`
if [ ${weekday} -eq 1 ];then
  echo "训练模型 ${model_day}"
  spark-submit --deploy-mode cluster --master yarn --driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} --conf spark.sql.shuffle.partitions=500  --conf spark.yarn.maxAppAttempts=1  --archives=oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/py370.zip#PY3  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 --conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 --conf spark.pyspark.driver.python=./PY3/py370/bin/python3 --conf spark.pyspark.python=./PY3/py370/bin/python3 oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/pyspark_gbdt.py --model_day ${model_day} --neg_pos_ratio 10 --train_days 7 --model_index ${index} --maxIter 20 --maxDepth  15 --minInfoGain 0 --minInstancesPerNode 200 --candidate_day  ${predict_day} --candidate_predict_index ${index} --test_day_sure ${model_test_day} --lastActivedays ${index}  --updatemodel 1
else
  echo "不进行模型的更新"
  spark-submit --deploy-mode cluster --master yarn --driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} --conf spark.sql.shuffle.partitions=500  --conf spark.yarn.maxAppAttempts=1  --archives=oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/py370.zip#PY3  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 --conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 --conf spark.pyspark.driver.python=./PY3/py370/bin/python3 --conf spark.pyspark.python=./PY3/py370/bin/python3 oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/pyspark_gbdt.py --model_day ${model_day} --neg_pos_ratio 10 --train_days 7 --model_index ${index} --maxIter 20 --maxDepth  15 --minInfoGain 0 --minInstancesPerNode 200 --candidate_day  ${predict_day} --candidate_predict_index ${index} --test_day_sure ${model_test_day} --lastActivedays ${index}  --updatemodel 0
fi

# 将driver端的输出进行保存
hdfs dfs -get -f oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/driver_applicationId_${index}.log
applicationId=`cat driver_applicationId_${index}.log`
yarn logs -applicationId ${applicationId} > driver_${index}.log
start_line=`awk '/logs_start/{print NR}' driver_${index}.log`
end_line=`awk '/logs_end/{print NR}' driver_${index}.log`
start_end="${start_line},${end_line}p"
sed -n "${start_end}" driver_${index}.log > oss_${predict_day}_candidate_predict_${index}.log
hdfs dfs -put -f oss_${predict_day}_candidate_predict_${index}.log oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/

# 将oss 数据保存至 s3
#rclone sync  oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/oss_20220628_candidate_predict_7_csv/ s3://emr-gift-sin-bj/model/social_app_predict/oss_20220628_candidate_predict_7_csv/  --progress --s3-chunk-size 64M --s3-upload-concurrency 16

