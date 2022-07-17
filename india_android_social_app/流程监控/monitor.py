import os
import datetime
def send_message(msg):
    # 测试
    # url = "https://open.feishu.cn/open-apis/bot/v2/hook/27567320-f7fe-4d8b-a2db-30414d1736d1"
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
                            "content": "India_Android_Soical_app 人群包自动化监控",
                            "tag": "plain_text"
                    }
                }
            }
        }'
        """ % (url, msg)
    os.system(command)
def get_file_ana():
    candidate_date = None
    model_date = None
    test_date = None
    test_sample_count = None
    train_auc = None
    test_auc = None
    predict_flag = False
    predict_bin_count = None
    predict_bin_number = 20
    bin_num = 0

    with open("candidate_predict.log", "r") as file:
        for f in file:
            if str(f).startswith("candidate_dirPath"):
                candidate_date=str(f).split("/")[-1].split("_")[1]
            if str(f).startswith("model_save_path"):
                model_date = str(f).split("/")[-1].split("_")[1]
            if str(f).startswith("args Namespace"):
                test_date = str(f).split("test_day_sure=")[1].split(",")[0].replace("'","")
            if str(f).__contains__("test_sample_count"):
                test_sample_count = str(f).split("test_sample_count:")[1].replace("\n","").replace(" ","")
            if str(f).startswith("train AUC"):
                train_auc = str(f).split("train AUC:")[1].replace("\n","").replace(" ","")
            if str(f).startswith("test AUC"):
                test_auc = str(f).split("test AUC:")[1].replace("\n","").replace(" ","")
            if str(f).__contains__("predict_df 分桶describe"):
                predict_flag = True
            if predict_flag:
                if str(f).__contains__("count"):
                    predict_bin_count = str(f).split("|")[2].replace("\n","").replace(" ","")
            if str(f).startswith(r"test_[0-9]{1,2} AUC:"):
                bin_num += 1
    oss_filePath = f"oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/{candidate_date}_candidate_predict_7.log"
    message = "人群包日期:%s\\n模型日期:%s\\n训练集AUC:%s\\n测试集日期:%s\\n测试集样本数:%s\\n测试集AUC:%s\\n人群包device_id个数:%d\\n详细日志请查看:%s"%(candidate_date,model_date,train_auc,test_date,test_sample_count,test_auc,predict_bin_number*int(predict_bin_count),oss_filePath)
    print(message)
    return message

if __name__ == '__main__':
    message = get_file_ana()
    send_message(message)
