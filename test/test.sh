driver_memory='10G'
executor_memory='50G'
num_executors=12
executor_cores=32

days_delay=2
geo="IDN"
platform="android"
oss_dirname="tiktok"
pkg_name="com.ss.android.ugc.trill"
pkg_subcategory="Video Players & Editors"
lastdaygap=3


model_day_temp=$((days_delay+2))
test_day_temp=$((days_delay+1))
train_days=5
test_days=1
neg_pos_ratio=50
maxDepth=15
maxIter=25
minInfoGain=0
minInstancesPerNode=50
predict_num=0
pkg_app_index=2


date_now=`date +%Y%m%d`
date_now="20220711"
echo "date_now:${date_now}"
model_day=`date -d "${date_now} -${model_day_temp} day" +%Y%m%d`
echo "model_day ${model_day}"
predict_day=`date -d"${date_now} -1 day" +%Y%m%d`
echo "predict_day ${predict_day}"
model_test_day=`date -d "${date_now} -${test_day_temp} day" +%Y%m%d`
echo "model_test_day ${model_test_day}"

oss_dirPath="oss://sdkemr-yeahmobi/user/chensheng/pkg/${oss_dirname}/${geo}_${platform}"

set -e

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

#echo "构建人群库 ${predict_day}"
#predict_log_name="${predict_day}_lastdaygap_${lastdaygap}.log"
#spark-submit \
#--deploy-mode cluster --master yarn  \
#--driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
#--conf spark.driver.memoryOverhead=1024  \
#--conf spark.executor.memoryOverhead=1024 \
#--conf spark.sql.shuffle.partitions=500 --conf spark.yarn.maxAppAttempts=1  \
#--archives=oss://sdkemr-yeahmobi/user/chensheng/py370docker.zip#PY3  \
#--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
#--conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
#--conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
#--conf spark.pyspark.python=./PY3/py370/bin/python3 \
#${oss_dirPath}/create_predict.py  \
#--sample_date ${predict_day} \
#--geo ${geo}  \
#--platform ${platform}  \
#--oss_dirname  ${oss_dirname}  \
#--pkg_name ${pkg_name}  \
#--pkg_subcategory "${pkg_subcategory}"  \
#--lastdaygap ${lastdaygap}
#
#hdfs dfs -get -f ${oss_dirPath}/predict/logs/driver_applicationId.log
#applicationId=`cat driver_applicationId.log`
#yarn logs -applicationId ${applicationId} > temp.log
#start_line=`awk '/logs_start/{print NR}' temp.log`
#end_line=`awk '/logs_end/{print NR}' temp.log`
#start_end="${start_line},${end_line}p"
#sed -n "${start_end}" temp.log > ${predict_log_name}
#hdfs dfs -put -f ${predict_log_name} ${oss_dirPath}/predict/logs/


model_test_day="20220708"
echo "构建样本 ${model_test_day}"
sample_log_name="${model_test_day}_delay_${days_delay}_lastdaygap_${lastdaygap}.log"
spark-submit \
--deploy-mode cluster --master yarn  \
--driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
--conf spark.driver.memoryOverhead=1024  \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.sql.shuffle.partitions=500  --conf spark.yarn.maxAppAttempts=1  \
--archives=oss://sdkemr-yeahmobi/user/chensheng/py370docker.zip#PY3  \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
--conf spark.pyspark.python=./PY3/py370/bin/python3 \
${oss_dirPath}/create_sample.py  \
--sample_date ${model_test_day} \
--days_delay ${days_delay}  \
--geo ${geo}  \
--platform ${platform}  \
--oss_dirname  ${oss_dirname}  \
--pkg_name ${pkg_name}  \
--pkg_subcategory "${pkg_subcategory}"  \
--lastdaygap ${lastdaygap}

hdfs dfs -get -f ${oss_dirPath}/sample/logs/driver_applicationId.log
applicationId=`cat driver_applicationId.log`
yarn logs -applicationId ${applicationId} > temp.log
start_line=`awk '/logs_start/{print NR}' temp.log`
end_line=`awk '/logs_end/{print NR}' temp.log`
start_end="${start_line},${end_line}p"
sed -n "${start_end}" temp.log > ${sample_log_name}
hdfs dfs -put -f ${sample_log_name} ${oss_dirPath}/sample/logs/



model_test_day="20220707"
echo "构建样本 ${model_test_day}"
sample_log_name="${model_test_day}_delay_${days_delay}_lastdaygap_${lastdaygap}.log"
spark-submit \
--deploy-mode cluster --master yarn  \
--driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
--conf spark.driver.memoryOverhead=1024  \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.sql.shuffle.partitions=500  --conf spark.yarn.maxAppAttempts=1  \
--archives=oss://sdkemr-yeahmobi/user/chensheng/py370docker.zip#PY3  \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
--conf spark.pyspark.python=./PY3/py370/bin/python3 \
${oss_dirPath}/create_sample.py  \
--sample_date ${model_test_day} \
--days_delay ${days_delay}  \
--geo ${geo}  \
--platform ${platform}  \
--oss_dirname  ${oss_dirname}  \
--pkg_name ${pkg_name}  \
--pkg_subcategory "${pkg_subcategory}"  \
--lastdaygap ${lastdaygap}

hdfs dfs -get -f ${oss_dirPath}/sample/logs/driver_applicationId.log
applicationId=`cat driver_applicationId.log`
yarn logs -applicationId ${applicationId} > temp.log
start_line=`awk '/logs_start/{print NR}' temp.log`
end_line=`awk '/logs_end/{print NR}' temp.log`
start_end="${start_line},${end_line}p"
sed -n "${start_end}" temp.log > ${sample_log_name}
hdfs dfs -put -f ${sample_log_name} ${oss_dirPath}/sample/logs/

model_test_day="20220706"
echo "构建样本 ${model_test_day}"
sample_log_name="${model_test_day}_delay_${days_delay}_lastdaygap_${lastdaygap}.log"
spark-submit \
--deploy-mode cluster --master yarn  \
--driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
--conf spark.driver.memoryOverhead=1024  \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.sql.shuffle.partitions=500  --conf spark.yarn.maxAppAttempts=1  \
--archives=oss://sdkemr-yeahmobi/user/chensheng/py370docker.zip#PY3  \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
--conf spark.pyspark.python=./PY3/py370/bin/python3 \
${oss_dirPath}/create_sample.py  \
--sample_date ${model_test_day} \
--days_delay ${days_delay}  \
--geo ${geo}  \
--platform ${platform}  \
--oss_dirname  ${oss_dirname}  \
--pkg_name ${pkg_name}  \
--pkg_subcategory "${pkg_subcategory}"  \
--lastdaygap ${lastdaygap}

hdfs dfs -get -f ${oss_dirPath}/sample/logs/driver_applicationId.log
applicationId=`cat driver_applicationId.log`
yarn logs -applicationId ${applicationId} > temp.log
start_line=`awk '/logs_start/{print NR}' temp.log`
end_line=`awk '/logs_end/{print NR}' temp.log`
start_end="${start_line},${end_line}p"
sed -n "${start_end}" temp.log > ${sample_log_name}
hdfs dfs -put -f ${sample_log_name} ${oss_dirPath}/sample/logs/



model_test_day="20220705"
echo "构建样本 ${model_test_day}"
sample_log_name="${model_test_day}_delay_${days_delay}_lastdaygap_${lastdaygap}.log"
spark-submit \
--deploy-mode cluster --master yarn  \
--driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
--conf spark.driver.memoryOverhead=1024  \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.sql.shuffle.partitions=500  --conf spark.yarn.maxAppAttempts=1  \
--archives=oss://sdkemr-yeahmobi/user/chensheng/py370docker.zip#PY3  \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
--conf spark.pyspark.python=./PY3/py370/bin/python3 \
${oss_dirPath}/create_sample.py  \
--sample_date ${model_test_day} \
--days_delay ${days_delay}  \
--geo ${geo}  \
--platform ${platform}  \
--oss_dirname  ${oss_dirname}  \
--pkg_name ${pkg_name}  \
--pkg_subcategory "${pkg_subcategory}"  \
--lastdaygap ${lastdaygap}

hdfs dfs -get -f ${oss_dirPath}/sample/logs/driver_applicationId.log
applicationId=`cat driver_applicationId.log`
yarn logs -applicationId ${applicationId} > temp.log
start_line=`awk '/logs_start/{print NR}' temp.log`
end_line=`awk '/logs_end/{print NR}' temp.log`
start_end="${start_line},${end_line}p"
sed -n "${start_end}" temp.log > ${sample_log_name}
hdfs dfs -put -f ${sample_log_name} ${oss_dirPath}/sample/logs/


model_test_day="20220704"
echo "构建样本 ${model_test_day}"
sample_log_name="${model_test_day}_delay_${days_delay}_lastdaygap_${lastdaygap}.log"
spark-submit \
--deploy-mode cluster --master yarn  \
--driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
--conf spark.driver.memoryOverhead=1024  \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.sql.shuffle.partitions=500  --conf spark.yarn.maxAppAttempts=1  \
--archives=oss://sdkemr-yeahmobi/user/chensheng/py370docker.zip#PY3  \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
--conf spark.pyspark.python=./PY3/py370/bin/python3 \
${oss_dirPath}/create_sample.py  \
--sample_date ${model_test_day} \
--days_delay ${days_delay}  \
--geo ${geo}  \
--platform ${platform}  \
--oss_dirname  ${oss_dirname}  \
--pkg_name ${pkg_name}  \
--pkg_subcategory "${pkg_subcategory}"  \
--lastdaygap ${lastdaygap}

hdfs dfs -get -f ${oss_dirPath}/sample/logs/driver_applicationId.log
applicationId=`cat driver_applicationId.log`
yarn logs -applicationId ${applicationId} > temp.log
start_line=`awk '/logs_start/{print NR}' temp.log`
end_line=`awk '/logs_end/{print NR}' temp.log`
start_end="${start_line},${end_line}p"
sed -n "${start_end}" temp.log > ${sample_log_name}
hdfs dfs -put -f ${sample_log_name} ${oss_dirPath}/sample/logs/


model_test_day="20220703"
echo "构建样本 ${model_test_day}"
sample_log_name="${model_test_day}_delay_${days_delay}_lastdaygap_${lastdaygap}.log"
spark-submit \
--deploy-mode cluster --master yarn  \
--driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
--conf spark.driver.memoryOverhead=1024  \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.sql.shuffle.partitions=500  --conf spark.yarn.maxAppAttempts=1  \
--archives=oss://sdkemr-yeahmobi/user/chensheng/py370docker.zip#PY3  \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
--conf spark.pyspark.python=./PY3/py370/bin/python3 \
${oss_dirPath}/create_sample.py  \
--sample_date ${model_test_day} \
--days_delay ${days_delay}  \
--geo ${geo}  \
--platform ${platform}  \
--oss_dirname  ${oss_dirname}  \
--pkg_name ${pkg_name}  \
--pkg_subcategory "${pkg_subcategory}"  \
--lastdaygap ${lastdaygap}

hdfs dfs -get -f ${oss_dirPath}/sample/logs/driver_applicationId.log
applicationId=`cat driver_applicationId.log`
yarn logs -applicationId ${applicationId} > temp.log
start_line=`awk '/logs_start/{print NR}' temp.log`
end_line=`awk '/logs_end/{print NR}' temp.log`
start_end="${start_line},${end_line}p"
sed -n "${start_end}" temp.log > ${sample_log_name}
hdfs dfs -put -f ${sample_log_name} ${oss_dirPath}/sample/logs/



#model_log_name="${predict_day}_delay_${days_delay}_lastdaygap_${lastdaygap}.log"
##每周二、周五更新一次模型
#weekday=`date -d "${date_now}" +%u`
#if [ ${weekday} -eq 2 ] || [ ${weekday} -eq 5 ];then
#  echo "训练模型 ${model_day}"
#  spark-submit \
#  --deploy-mode cluster \
#  --master yarn    \
#  --driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
#  --conf spark.driver.memoryOverhead=1024  \
#  --conf spark.executor.memoryOverhead=1024 \
#  --conf spark.sql.shuffle.partitions=100  \
#  --archives=oss://sdkemr-yeahmobi/user/chensheng/py370docker.zip#PY3  \
#  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
#  --conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
#  --conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
#  --conf spark.pyspark.python=./PY3/py370/bin/python3 \
#  ${oss_dirPath}/pyspark_gbdt.py \
#  --model_day ${model_day} \
#  --days_delay ${days_delay} \
#  --geo ${geo}  \
#  --platform ${platform}  \
#  --oss_dirname ${oss_dirname}  \
#  --pkg_name ${pkg_name}  \
#  --pkg_subcategory "${pkg_subcategory}"  \
#  --lastdaygap ${lastdaygap}  \
#  --train_days ${train_days}  \
#  --test_days ${test_days}  \
#  --neg_pos_ratio ${neg_pos_ratio}  \
#  --maxDepth ${maxDepth}  \
#  --maxIter ${maxIter}  \
#  --minInfoGain ${minInfoGain}  \
#  --minInstancesPerNode ${minInstancesPerNode}  \
#  --predict_day ${predict_day}  \
#  --predict_num ${predict_num}  \
#  --updatemodel 1 \
#  --pkg_app_index ${pkg_app_index}
#else
#    echo "不进行模型的更新"
#    spark-submit \
#  --deploy-mode cluster \
#  --master yarn    \
#  --driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
#  --conf spark.driver.memoryOverhead=1024  \
#  --conf spark.executor.memoryOverhead=1024 \
#  --conf spark.sql.shuffle.partitions=100  \
#  --archives=oss://sdkemr-yeahmobi/user/chensheng/py370docker.zip#PY3  \
#  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
#  --conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
#  --conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
#  --conf spark.pyspark.python=./PY3/py370/bin/python3 \
#  ${oss_dirPath}/pyspark_gbdt.py \
#  --model_day ${model_day} \
#  --days_delay ${days_delay} \
#  --geo ${geo}  \
#  --platform ${platform}  \
#  --oss_dirname ${oss_dirname}  \
#  --pkg_name ${pkg_name}  \
#  --pkg_subcategory "${pkg_subcategory}"  \
#  --lastdaygap ${lastdaygap}  \
#  --train_days ${train_days}  \
#  --test_days ${test_days}  \
#  --neg_pos_ratio ${neg_pos_ratio}  \
#  --maxDepth ${maxDepth}  \
#  --maxIter ${maxIter}  \
#  --minInfoGain ${minInfoGain}  \
#  --minInstancesPerNode ${minInstancesPerNode}  \
#  --predict_day ${predict_day}  \
#  --predict_num ${predict_num}  \
#  --updatemodel 0 \
#  --pkg_app_index ${pkg_app_index}
#fi
#
## 将driver端的输出进行保存
#hdfs dfs -get -f ${oss_dirPath}/model/logs/driver_applicationId.log
#applicationId=`cat driver_applicationId.log`
#yarn logs -applicationId ${applicationId} > temp.log
#start_line=`awk '/logs_start/{print NR}' temp.log`
#end_line=`awk '/logs_end/{print NR}' temp.log`
#start_end="${start_line},${end_line}p"
#sed -n "${start_end}" temp.log > ${model_log_name}
#hdfs dfs -put -f ${model_log_name} ${oss_dirPath}/model/logs/