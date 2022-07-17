#date_now=`date +%Y%m%d`
date_now="20220701"
echo "date_now:${date_now}"

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

set -e

spark-submit \
--deploy-mode cluster \
--master yarn    \
--driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors ${num_executors} --executor-cores ${executor_cores} \
--conf spark.driver.memoryOverhead=1024  \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.sql.shuffle.partitions=100  \
--archives=oss://sdkemr-yeahmobi/user/chensheng/py370docker.zip#PY3  \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
--conf spark.pyspark.python=./PY3/py370/bin/python3 \
oss://sdkemr-yeahmobi/user/chensheng/pkg/${oss_dirname}/${geo}_${platform}/data_ana.py \
--sample_date ${date_now} \
--days_delay ${days_delay}  \
--geo ${geo}  \
--platform ${platform}  \
--oss_dirname  ${oss_dirname}  \
--pkg_name ${pkg_name}  \
--pkg_subcategory ${pkg_subcategory}





