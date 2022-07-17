set -e
sample_date=$1
pos="rclone  sync oss:sdkemr-yeahmobi/user/chensheng/sample_create_social_app/oss_${sample_date}_pos_sample_delay_2/ s3://emr-gift-sin-bj/model/sample_create_social_app/oss_${sample_date}_pos_sample_delay_2/ --progress --s3-chunk-size 64M --s3-upload-concurrency 16"
neg="rclone  sync oss:sdkemr-yeahmobi/user/chensheng/sample_create_social_app/oss_${sample_date}_neg_sample_delay_2/ s3://emr-gift-sin-bj/model/sample_create_social_app/oss_${sample_date}_neg_sample_delay_2/ --progress --s3-chunk-size 64M --s3-upload-concurrency 16"
#echo ${pos}
echo ${neg}