import json
import os
import datetime
import argparse
import re
import sys
import pandas as pd
import json

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

def add_message(message,name,data):
    message_2 = f"{message}{name}:{data}\\n"
    return message_2

def get_message():
    monitor_dict_str = ""
    s3_root_path = "s3://emr-gift-sin-bj/model/chensheng/pkg"
    oss_root_path = "oss://sdkemr-yeahmobi/user/chensheng/pkg"
    with open("model_predict.log", "r") as file:
        for f in file:
            if str(f).startswith("montor_dict"):
                monitor_dict_str = str(f).split("{")[1].split("}")[0].replace(" ","").replace("'","\"")
    print(monitor_dict_str)
    monitor_dict = json.loads("{"+monitor_dict_str+"}")
    print(monitor_dict)
    predict_date = str(monitor_dict["predict_day"])
    model_need_only = int(monitor_dict["model_need_only"])
    train_label_temp_12_AUC=None
    test_label_temp_12_AUC=None
    train_label_temp_1_AUC=None
    test_label_temp_1_AUC = None
    if model_need_only==1:
        model_dir = str(monitor_dict["model_save_path_need"])
        train_label_temp_12_AUC = str(monitor_dict["is_need_True_train_label_temp_12_auc"])
        test_label_temp_12_AUC = str(monitor_dict["is_need_True_test_label_temp_12_auc"])
        train_label_temp_1_AUC = str(monitor_dict["is_need_True_train_label_temp_1_auc"])
        test_label_temp_1_AUC = str(monitor_dict["is_need_True_test_label_temp_1_auc"])

    else:
        model_dir = str(monitor_dict["model_save_path"])
        train_label_temp_12_AUC = str(monitor_dict["is_need_False_train_label_temp_12_auc"])
        test_label_temp_12_AUC = str(monitor_dict["is_need_False_test_label_temp_12_auc"])
        train_label_temp_1_AUC = str(monitor_dict["is_need_False_train_label_temp_1_auc"])
        test_label_temp_1_AUC = str(monitor_dict["is_need_False_test_label_temp_1_auc"])
    test_date = str(monitor_dict["test_pos_neg_sample_list"][0].split("/")[-1].split("_")[0])
    test_sample_label_temp_1_count = str(monitor_dict["test_sample_0_label_temp_1_count"])
    test_sample_label_temp_2_count = str(monitor_dict["test_sample_0_label_temp_2_count"])
    test_sample_neg_count = str(monitor_dict["test_sample_0_neg_count"])
    predict_count = str(monitor_dict["predict_count"])
    predict_dirPath = str(monitor_dict["predict_dirPath"]).replace(oss_root_path,s3_root_path)

    days_delay=str(monitor_dict["days_delay"])
    lastdaygap=str(monitor_dict["lastdaygap"])
    model_suffix=str(monitor_dict["model_suffix"])
    oss_dirname = str(monitor_dict["oss_dirname"])
    geo = str(monitor_dict["geo"])
    platform = str(monitor_dict["platform"])
    model_log_name = f"{predict_date}_delay_{days_delay}_lastdaygap_{lastdaygap}{model_suffix}.log"
    model_log_dirPath = f"{oss_root_path}/{oss_dirname}/{geo}_{platform}/model/logs/{model_log_name}"

    message = ""
    message = add_message(message,"人群包日期",predict_date)
    message = add_message(message,"人群包数量",predict_count)
    message = add_message(message,"人群包地址",predict_dirPath)
    message = add_message(message,"模型地址",model_dir)
    message = add_message(message,"训练集AUC",train_label_temp_12_AUC)
    message = add_message(message,"训练集label_temp_1 AUC",train_label_temp_1_AUC)
    message = add_message(message,"测试集日期",test_date)
    message = add_message(message,"测试集AUC",test_label_temp_12_AUC)
    message = add_message(message,"测试集label_temp_1 AUC",test_label_temp_1_AUC)
    message = add_message(message,"测试集label_temp_1_count",test_sample_label_temp_1_count)
    message = add_message(message,"测试集label_temp_2_count",test_sample_label_temp_2_count)
    message = add_message(message,"测试集负样本数量",test_sample_neg_count)
    message = add_message(message,"测试集模型评价日志地址",model_log_dirPath)
    print(message)
    return message

if __name__ == '__main__':
    is_test=1
    message_head="test"
    try:
        is_test=int(sys.argv[1])
        message_head = str(sys.argv[2])
    except Exception as e:
        print(e)
    print("is_test",is_test)
    message = get_message()
    send_message(message,message_head)

