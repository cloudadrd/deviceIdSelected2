import os
import datetime
import argparse
import re
import sys
import pandas as pd

def send_message(msg,content):
    if is_test:
        # 测试
        url = "https://open.feishu.cn/open-apis/bot/v2/hook/27567320-f7fe-4d8b-a2db-30414d1736d1"
    else:
        # 正式
        url =  "https://open.feishu.cn/open-apis/bot/v2/hook/f08b1f5d-bc50-40a6-be0b-ebe2fda5da3a"
    print(msg)
    msg = msg.replace("\n","\\n")
    command = """
       curl '%s' \
        -H 'Content-Type: application/json' \
        -d '{
            "msg_type": "interactive",
            "card": {
                "config": {
                    "wide_screen_mode": true,
                    "enable_forward": true
                },
                "elements": [{
                    "tag": "div",
                    "text": {
                        "content": "%s",
                        "tag": "lark_md"
                    }
                }],
                "header": {
                    "title": {
                            "content": "%s",
                            "tag": "plain_text"
                    }
                }
            }
        }'
        """ % (url, msg,content)
    os.system(command)
def get_file_ana():
    predict_date = None
    model_date = None
    test_date = None
    test_sample_1_count = None
    test_sample_0_count = None
    train_auc_label_temp_12 = None
    train_auc_label_temp_1 = None
    test_auc_label_temp_12 = None
    test_auc_label_temp_1 = None
    predict_path="s3://emr-gift-sin-bj/model/chensheng/pkg/"
    geo=None
    platform=None
    oss_dirname=None
    days_delay=None
    lastdaygap=None



    bin_num = None
    predict_bin_per_cnt = None

    test_pos_neg_sample_list_flag=False
    test_label_1_flag=False
    test_label_0_flag=False
    predict_flag=False



    with open("model_predict.log", "r") as file:
        for f in file:
            if str(f).startswith("predict_predict_dirPath"):
                predict_date=str(f).split("/")[-1].split("_")[0]
                predict_path += str(f).split("chensheng/pkg/")[1].replace(" ","").replace("\n","")
            if str(f).startswith("model_save_path_need"):
                model_date = str(f).split("/")[-1].split("_")[0]
            if str(f).startswith("test_pos_neg_sample_list"):
                test_pos_neg_sample_list_flag=True
            if test_pos_neg_sample_list_flag and str(f).__contains__("sample"):
                test_date=re.findall("[0-9]*",str(f))[0]
                test_pos_neg_sample_list_flag=False
            if str(f).startswith("test rawPrediction_1_desc:label=1"):
                test_label_1_flag=True
            if test_label_1_flag and str(f).__contains__("count"):
                test_sample_1_count=str(f).split("|")[2].replace(" ","").replace("\n","")
                test_label_1_flag = False

            if str(f).startswith("test rawPrediction_1_desc:label=0"):
                test_label_0_flag=True
            if test_label_0_flag and str(f).__contains__("count"):
                test_sample_0_count=str(f).split("|")[2].replace(" ","").replace("\n","")
                test_label_0_flag = False


            if str(f).startswith("train AUC") and  str(f).__contains__("label_temp=2"):
                train_auc_label_temp_12 = str(f).split("train AUC:")[1].split("label_temp")[0].replace("\n","").replace(" ","")
            if str(f).startswith("train AUC") and not str(f).__contains__("label_temp=2"):
                train_auc_label_temp_1 = str(f).split("train AUC:")[1].split("label_temp")[0].replace("\n","").replace(" ","")
            if str(f).startswith("test AUC") and str(f).__contains__("label_temp=2"):
                print(f)
                test_auc_label_temp_12 = str(f).split("test AUC:")[1].split("label_temp")[0].replace("\n","").replace(" ","")
            if str(f).startswith("test AUC") and not str(f).__contains__("label_temp=2"):
                print(f)
                test_auc_label_temp_1 = str(f).split("test AUC:")[1].split("label_temp")[0].replace("\n","").replace(" ","")


            if str(f).__contains__("bin_num"):
                bin_num=int(str(f).split("bin_num=")[1].split(",")[0].replace("\n","").replace(" ",""))
                oss_dirname=str(f).split("oss_dirname=")[1].split(",")[0].replace("\n","").replace(" ","").replace("'","")
                geo=str(f).split("geo=")[1].split(",")[0].replace("\n","").replace(" ","").replace("'","")
                platform=str(f).split("platform=")[1].split(",")[0].replace("\n","").replace(" ","").replace("'","")
                days_delay=str(f).split("days_delay=")[1].split(",")[0].replace("\n","").replace(" ","").replace("'","")
                lastdaygap=str(f).split("lastdaygap=")[1].split(",")[0].replace("\n","").replace(" ","").replace("'","")
            if str(f).startswith("针对人群库进行评分"):
                predict_flag=True
            if predict_flag and str(f).__contains__("count"):
                predict_bin_per_cnt=int(str(f).split("|")[2].replace(" ","").replace("\n",""))
                predict_flag=False

    oss_log_filePath = f"oss://sdkemr-yeahmobi/user/chensheng/pgk/{oss_dirname}/{geo}_{platform}/model/logs/{predict_date}_delay_{days_delay}_lastdaygap_{lastdaygap}.log"
    message = f"人群包日期:{predict_date}\\n模型日期:{model_date}\\n训练集AUC:{train_auc_label_temp_12}\\n训练集label_temp=1 AUC:{train_auc_label_temp_1}\\n测试集日期:{test_date}\\n测试集正样本数:{test_sample_1_count}\\n测试集负样本数:{test_sample_0_count}\\n测试集AUC:{test_auc_label_temp_12}\\n测试集合label_temp=1 AUC:{test_auc_label_temp_1}\\n人群包device_id个数:{bin_num*predict_bin_per_cnt}\\n人群包s3地址:{predict_path}\\n详细日志请查看:{oss_log_filePath}"
    print(message)
    return message



if __name__ == '__main__':
    is_test=1
    try:
        is_test=int(sys.argv[1])
    except Exception as e:
        print(e)
    print("is_test",is_test)
    message = get_file_ana()
    send_message(message,"tiktok_IDN_Android 人群包自动化监控")

