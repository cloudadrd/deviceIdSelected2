set -e
spark-submit \
--deploy-mode cluster \
--master yarn    \
--driver-memory 5G --executor-memory 50G --num-executors 6 --executor-cores 16 \
--conf spark.driver.memoryOverhead=1024  \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.sql.shuffle.partitions=1000 \
--conf spark.yarn.maxAppAttempts=1  \
--archives=oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/py370.zip#PY3  \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.executorEnv.PYSPARK_PYTHON=./PY3/py370/bin/python3 \
--conf spark.pyspark.driver.python=./PY3/py370/bin/python3 \
--conf spark.pyspark.python=./PY3/py370/bin/python3 \
oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/pyspark_gbdt.py --model_index 4