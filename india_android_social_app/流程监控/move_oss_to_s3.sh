cd /mnt/chensheng/pkg/social_1/IND_android
# 陈胜
# #chensheng 将social_app人群包由oss复制到s3
#20 12 * * * sh /mnt/chensheng/pkg/social_1/IND_android/shell.sh > /mnt/chensheng/pkg/social_1/IND_android/shell.log
date_now=`date +%Y%m%d`
echo "date_now:${date_now}"
predict_day=`date -d"${date_now} -1 day" +%Y%m%d`
echo "predict_day ${predict_day}"
index=7
oss="oss:sdkemr-yeahmobi/user/chensheng/sample_create_social_app/oss_${predict_day}_candidate_predict_${index}_csv/"
s3="s3://emr-gift-sin-bj/model/social_app_predict/oss_${predict_day}_candidate_predict_${index}_csv/"
exits_success="oss:sdkemr-yeahmobi/user/chensheng/sample_create_social_app/oss_${predict_day}_candidate_predict_${index}_csv/_SUCCESS"
is_exist=0
while [ ${is_exist} -eq 0 ]
do
    ls_exist=`rclone ls  ${exits_success}`
    if [ ! $ls_exist ]; then
      echo "${exits_success} not exists!"
      sleep 5m
    else
      echo "${exits_success}  exists!"
      is_exist=1
    fi
done
set -e

rclone sync  ${oss} ${s3}  --progress --s3-chunk-size 64M --s3-upload-concurrency 16
sleep 5m
echo "ok" > upload.log
rclone sync upload.log ${s3}

rclone sync oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/${predict_day}_candidate_predict_7.log ./
mv ${predict_day}_candidate_predict_7.log candidate_predict.log

python_cs=/mnt/chensheng/py370/bin/python
$python_cs monitor.py


