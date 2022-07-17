days_delay=1
geo="IDN"
platform="android"
oss_dirname="lazada"
lastdaygap=7

message_head="tiktok_IDN_Android 人群包自动化监控"
cd /mnt/chensheng/pkg/${oss_dirname}/${geo}_${platform}
# 陈胜
# #chensheng 将social_app人群包由oss复制到s3
date_now=`date +%Y%m%d`
echo "date_now:${date_now}"
predict_day=`date -d"${date_now} -1 day" +%Y%m%d`
echo "predict_day ${predict_day}"

oss_dirPath="oss://sdkemr-yeahmobi/user/chensheng/pkg/${oss_dirname}/${geo}_${platform}"
s3_dirPath="s3://emr-gift-sin-bj/model/chensheng/pkg/${oss_dirname}/${geo}_${platform}"
oss="${oss_dirPath}/predict/${predict_day}_lastdaygap_${lastdaygap}_predict/"
s3="${s3_dirPath}/predict/${predict_day}_lastdaygap_${lastdaygap}_predict/"
exits_success="${oss}_SUCCESS"
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

model_log_name="${predict_day}_delay_${days_delay}_lastdaygap_${lastdaygap}.log"
rclone sync ${oss_dirPath}/model/logs/${model_log_name} ./

mv ${model_log_name} model_predict.log

python_cs=/mnt/chensheng/py370/bin/python
$python_cs /mnt/chensheng/pkg/monitor.py 0 "${message_head}"
rm -f model_predict.log

