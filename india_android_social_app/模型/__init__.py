
import os
import re

sample_dirPath = "oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app"
model_index=7
command='hdfs dfs -ls -d %s/model_[0-9]*_%d > model_str.txt'%(sample_dirPath,model_index)
os.system(command)
os.system("cat model_str.txt")

pattern = re.compile(r"model_[0-9]*_%d" % model_index)
pattern.findall(model_str)