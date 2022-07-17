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
executor_memory='50G'
num_executors=12
executor_cores=16
pkg_app_index=1
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
spark-submit \
--deploy-mode cluster --master yarn  \
--driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
--conf spark.driver.memoryOverhead=1024  \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.sql.shuffle.partitions=500 --conf spark.yarn.maxAppAttempts=1  \
--archives=oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/py370.zip#PY3  \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
--conf spark.pyspark.python=./PY3/py370/bin/python3 \
oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/create_candidate_data.py  --sample_date ${predict_day} --lastActivedays ${index}

hdfs dfs -get -f oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/driver_applicationId.log
applicationId=`cat driver_applicationId.log`
yarn logs -applicationId ${applicationId} > temp.log
start_line=`awk '/logs_start/{print NR}' temp.log`
end_line=`awk '/logs_end/{print NR}' temp.log`
start_end="${start_line},${end_line}p"
sed -n "${start_end}" temp.log > ${predict_day}_candidate_${index}.log
hdfs dfs -put -f ${predict_day}_candidate_${index}.log oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/


echo "构建样本 ${model_test_day}"
spark-submit \
--deploy-mode cluster --master yarn  \
--driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
--conf spark.driver.memoryOverhead=1024  \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.sql.shuffle.partitions=500  --conf spark.yarn.maxAppAttempts=1  \
--archives=oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/py370.zip#PY3  \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
--conf spark.pyspark.python=./PY3/py370/bin/python3 \
oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/create_social_ana_sample_from_oss_delay.py  --sample_date ${model_test_day} --lastActivedays ${index}

hdfs dfs -get -f oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/driver_applicationId.log
applicationId=`cat driver_applicationId.log`
yarn logs -applicationId ${applicationId} > temp.log
start_line=`awk '/logs_start/{print NR}' temp.log`
end_line=`awk '/logs_end/{print NR}' temp.log`
start_end="${start_line},${end_line}p"
sed -n "${start_end}" temp.log > ${model_test_day}_sample_${index}.log
hdfs dfs -put -f ${model_test_day}_sample_${index}.log oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/



#每周二、周五更新一次模型
weekday=`date -d "${date_now}" +%u`
if [ ${weekday} -eq 2 ] || [ ${weekday} -eq 5 ];then
  echo "训练模型 ${model_day}"
  spark-submit \
  --deploy-mode cluster \
  --master yarn    \
  --driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
  --conf spark.driver.memoryOverhead=1024  \
  --conf spark.executor.memoryOverhead=1024 \
  --conf spark.sql.shuffle.partitions=100  \
  --archives=oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/py370.zip#PY3  \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
  --conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
  --conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
  --conf spark.pyspark.python=./PY3/py370/bin/python3 \
  oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/pyspark_gbdt.py \
  --model_day ${model_day} \
  --neg_pos_ratio 20 \
  --train_days 7 \
  --model_index ${index} \
  --maxIter 20 \
  --maxDepth  10 \
  --minInfoGain 0 \
  --minInstancesPerNode 200 \
  --candidate_day  ${predict_day} \
  --candidate_predict_index ${index} \
  --test_day_sure ${model_test_day} \
  --lastActivedays ${index}  \
  --updatemodel 1 \
  --pkg_app_index ${pkg_app_index}
else
  echo "不进行模型的更新"
  spark-submit \
  --deploy-mode cluster \
  --master yarn    \
  --driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
  --conf spark.driver.memoryOverhead=1024  \
  --conf spark.executor.memoryOverhead=1024 \
  --conf spark.sql.shuffle.partitions=100  \
  --archives=oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/py370.zip#PY3  \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
  --conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
  --conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
  --conf spark.pyspark.python=./PY3/py370/bin/python3 \
  oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/pyspark_gbdt.py \
  --model_day ${model_day} \
  --neg_pos_ratio 10 \
  --train_days 7 \
  --model_index ${index} \
  --maxIter 30 \
  --maxDepth  15 \
  --minInfoGain 0 \
  --minInstancesPerNode 200 \
  --candidate_day  ${predict_day} \
  --candidate_predict_index ${index} \
  --test_day_sure ${model_test_day} \
  --lastActivedays ${index}  \
  --updatemodel 0 \
  --pkg_app_index ${pkg_app_index}
fi

# 将driver端的输出进行保存
hdfs dfs -get -f oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/driver_applicationId.log
applicationId=`cat driver_applicationId.log`
yarn logs -applicationId ${applicationId} > temp.log
start_line=`awk '/logs_start/{print NR}' temp.log`
end_line=`awk '/logs_end/{print NR}' temp.log`
start_end="${start_line},${end_line}p"
sed -n "${start_end}" temp.log > ${predict_day}_candidate_predict_${index}.log
hdfs dfs -put -f ${predict_day}_candidate_predict_${index}.log oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/



