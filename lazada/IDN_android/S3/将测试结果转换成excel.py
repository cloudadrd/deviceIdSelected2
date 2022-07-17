import os
import sys
import re
import pandas as pd
def get_lastest_log():
    f=os.walk("./")
    log_files = []
    for dirpath,dirnames,filenames in f:
        for file in filenames:
            if len(re.findall("\d*_delay_2_lastdaygap_3\.log",file)):
                log_files.append(file)
    log_files_2 = sorted(log_files,reverse=True)
    return log_files_2[0]

def ana(log_file):
    test_predictions_label_bin_flag=False
    header_flag=False
    header_list = None
    data_list = []
    with open(log_file,"r") as f:
        for line in f:
            if str(line).__contains__("test_predictions_label_bin"):
                test_predictions_label_bin_flag=True
            if test_predictions_label_bin_flag and not header_flag and str(line).__contains__("|"):
                header_list = [data  for data in str(line).split("|") if data not in ("","\n")]
                header_flag=True
            elif test_predictions_label_bin_flag and  header_flag  and str(line).__contains__("|"):
                data_list_temp = [float(data) for data in str(line).split("|") if data not in ("","\n")]
                data_list.append(data_list_temp)
            elif test_predictions_label_bin_flag and  header_flag and len(data_list)>0 and not str(line).__contains__("|"):
                test_predictions_label_bin_flag = False
            else:
                pass

    df = pd.DataFrame(data=data_list,columns=header_list)
    df.to_excel(f"{log_name}.xlsx",header=True,index=False)




if __name__ == '__main__':
    log_file = get_lastest_log()
    log_name = log_file.split(".")[0]
    print(log_file)
    ana(log_file)