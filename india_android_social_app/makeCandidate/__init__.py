f=open("run.sh", "r")
model_str=f.readline()
print(model_str)
import re
a=re.findall("model_[0-9]*_%d"%0,model_str)
print(a)
b=sorted(a,reverse=True)
print(b)