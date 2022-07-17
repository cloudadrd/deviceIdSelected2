driver_memory='10G'
executor_memory='50G'
num_executors=12
executor_cores=32

days_delay=2
geo="IDN"
platform="android"
oss_dirname="lazada"
pkg_name="com.lazada.android"
pkg_subcategory="Shopping"
lastdaygap=3


oss_dirPath="oss://sdkemr-yeahmobi/user/chensheng/pkg/${oss_dirname}/${geo}_${platform}"


model_test_day="20220630"
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
--pkg_subcategory ${pkg_subcategory}  \
--lastdaygap ${lastdaygap}
#
hdfs dfs -get -f ${oss_dirPath}/sample/logs/driver_applicationId.log
applicationId=`cat driver_applicationId.log`
yarn logs -applicationId ${applicationId} > temp.log
start_line=`awk '/logs_start/{print NR}' temp.log`
end_line=`awk '/logs_end/{print NR}' temp.log`
start_end="${start_line},${end_line}p"
sed -n "${start_end}" temp.log > ${sample_log_name}
hdfs dfs -put -f ${sample_log_name} ${oss_dirPath}/sample/logs/


model_test_day="20220629"
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
--pkg_subcategory ${pkg_subcategory}  \
--lastdaygap ${lastdaygap}
#
hdfs dfs -get -f ${oss_dirPath}/sample/logs/driver_applicationId.log
applicationId=`cat driver_applicationId.log`
yarn logs -applicationId ${applicationId} > temp.log
start_line=`awk '/logs_start/{print NR}' temp.log`
end_line=`awk '/logs_end/{print NR}' temp.log`
start_end="${start_line},${end_line}p"
sed -n "${start_end}" temp.log > ${sample_log_name}
hdfs dfs -put -f ${sample_log_name} ${oss_dirPath}/sample/logs/

