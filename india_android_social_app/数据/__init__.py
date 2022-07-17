"""
一、第一次实验：
1.样本
(1) 正样本：最近一段时间（比如三个月）安装这款应用的集合
(2) 负样本：安装列表里面没有这款应用的设备，训练的时候随机采样
正样本数据量：209265365
负样本数据量：163878369
2.特征
(1) os_ver_name 操作系统版本
(2) language 操作系统语言
(3) install_bundle_size  安装列表长度
(4) user_bundle_list 安装列表
3.模型
SVM
4.问题
(1) 特征缺失 osv、language
(2) 样本：没有投放信息！

二、第二次实验
1.样本
(1) 负样本：当天活跃的没有安装这三个包的设备。
正样本数据量：31780046 ？？？
负样本数据量：168697386 ？？？
(2)采样：
    正样本数据量：6356804 ？？？
    负样本数据量：3178402 ？？？
3.模型
(1)GBDT
(2)分包训练


1.样本
(1) 正样本：最近一个月有安装应用的用户

四、第三次实验
1.样本
(1)正样本由一个月活跃改成最近14天活跃
(2)增量样本7天 -- 保证两遍候选集一致？？
2.特征
(1)添加活跃天数
(2)添加兴趣标签

"""
import pandas as pd
# print(df)
df_2 = df[["num","label_rule","label_rule_2","label_rule_3"]]
dict_temp_1 = {"a":1,"b":2,"c":3,"d":4}
dict_temp_2 = {"w":1,"x":2,"y":3,"z":4}
df_2["label_rule_abcd"]=[dict_temp_1[data[0]]  if data is not None else None for data in df_2["label_rule"].values.tolist()]
df_2["label_rule_wxyz"]=[dict_temp_2[data[1]]  if data is not None else None for data in df_2["label_rule"].values.tolist()]
df_2["label_rule_12"]=[int(data[2])  if data is not None else None for data in df_2["label_rule"].values.tolist()]

df_2["label_rule_2_abcd"]=[dict_temp_1[data[0]]  if data is not None else None for data in df_2["label_rule_2"].values.tolist()]
df_2["label_rule_2_wxyz"]=[dict_temp_2[data[1]]  if data is not None else None for data in df_2["label_rule_2"].values.tolist()]
df_2["label_rule_2_12"]=[int(data[2])  if data is not None else None for data in df_2["label_rule_2"].values.tolist()]

df_2["label_rule_3_abcd"]=[dict_temp_1[data[0]]  if not pd.isna(data) else None for data in df_2["label_rule_3"].values.tolist()]
df_2["label_rule_3_wxyz"]=[dict_temp_2[data[1]]  if not pd.isna(data) else None for data in df_2["label_rule_3"].values.tolist()]
df_2["label_rule_3_12"]=[int(data[2])  if not pd.isna(data) else None for data in df_2["label_rule_3"].values.tolist()]

print(df_2)
df_2.to_excel("test.xlsx")
