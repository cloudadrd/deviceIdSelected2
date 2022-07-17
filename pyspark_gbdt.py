# -*- coding: utf-8 -*-
import os,sys
import argparse
import datetime
from time import strftime, localtime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, udf, array_except, col, array, array_contains, row_number,rand
from pyspark.sql.types import DoubleType
import warnings
warnings.filterwarnings('ignore')
from pyspark.ml import Pipeline,PipelineModel
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import StringIndexer, CountVectorizer, VectorAssembler, OneHotEncoder, QuantileDiscretizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
import re
second_element=udf(lambda v:float(v[1]), DoubleType())
def eval_metric(data_type,prediction,prediction_col="rawPrediction_1",evaluate_auc=True,is_need=True):
    print("*"*20)
    print("%s %s_desc"%(data_type,prediction_col))
    print("%s %s_desc:label=1" % (data_type,prediction_col))
    prediction.select(prediction_col).where("label=1").describe().show()
    print("%s %s_desc:label=0" % (data_type,prediction_col))
    prediction.select(prediction_col).where("label=0").describe().show()
    if evaluate_auc:
        evaluator = BinaryClassificationEvaluator(rawPredictionCol=prediction_col)
        label_temp_12_auc = evaluator.evaluate(prediction)
        print('%s AUC: %.6f label_temp=1 and label_temp=2' % (data_type, label_temp_12_auc))
        prediction_temp_1 = prediction.where("label_temp=1 or label_temp=3")
        label_temp_1_auc = evaluator.evaluate(prediction_temp_1)
        print('%s AUC: %.6f label_temp=1' % (data_type, label_temp_1_auc))
        monitor_dict[f"is_need_{is_need}_{data_type}_label_temp_12_auc"]=label_temp_12_auc
        monitor_dict[f"is_need_{is_need}_{data_type}_label_temp_1_auc"]=label_temp_1_auc


def create_data(pos_neg_sample_list,is_train=True,is_need=True):
    if is_train:
        print(f"************创建训练集 is_need {is_need}****************")
        train_sample = None
        for i in range(len(pos_neg_sample_list)):
            sample_df_i = spark.read.format("orc").load(pos_neg_sample_list[i])

            pos_sample_df_i_label_temp_1 = sample_df_i.where("label_temp=1").withColumn("label", lit(1))
            pos_sample_df_i_label_temp_1_count = pos_sample_df_i_label_temp_1.count()
            pos_sample_df_i_label_temp_2 = sample_df_i.where("label_temp=2").withColumn("label", lit(1))
            pos_sample_df_i_label_temp_2_count = pos_sample_df_i_label_temp_2.count()
            if is_need:
                if label_temp_2_ratio != -1:
                    pos_sample_df_i_label_temp_2_sampling = pos_sample_df_i_label_temp_2.sample(pos_sample_df_i_label_temp_1_count*label_temp_2_ratio/pos_sample_df_i_label_temp_2_count)
                else:
                    pos_sample_df_i_label_temp_2_sampling = pos_sample_df_i_label_temp_2
                pos_sample_df_i_label_temp_2_sampling_count = pos_sample_df_i_label_temp_2_sampling.count()
                pos_sample_df_i = pos_sample_df_i_label_temp_1.unionAll(pos_sample_df_i_label_temp_2_sampling)

            else:
                pos_sample_df_i_label_temp_2_sampling_count=0
                pos_sample_df_i = pos_sample_df_i_label_temp_1
            pos_sample_df_i_count = pos_sample_df_i.count()
            print(f"pos_sample_df_i_label_temp_1_count:{pos_sample_df_i_label_temp_1_count},pos_sample_df_i_label_temp_2_count:{pos_sample_df_i_label_temp_2_count},pos_sample_df_i_count:{pos_sample_df_i_count}")
            monitor_dict[f"train_sample_{i}_label_temp_1_count"]=pos_sample_df_i_label_temp_1_count
            monitor_dict[f"train_sample_{i}_label_temp_2_count"]=pos_sample_df_i_label_temp_2_count
            monitor_dict[f"is_need_{is_need}_train_sample_{i}_label_temp_2_sampling_count"]=pos_sample_df_i_label_temp_2_sampling_count
            monitor_dict[f"is_need_{is_need}_train_sample_{i}_pos_count"]=pos_sample_df_i_count


            neg_sample_df_i = sample_df_i.where("label_temp=3").withColumn("label",lit(0))
            neg_sample_df_i_count = neg_sample_df_i.count()
            print(f"train_sample_i:{i},pos_count:{pos_sample_df_i_count},neg_count:{neg_sample_df_i_count}")

            neg_sample_df_i_sampling = neg_sample_df_i.sample(pos_sample_df_i_count*neg_pos_ratio/neg_sample_df_i_count)
            neg_sample_df_i_sampling_count = neg_sample_df_i_sampling.count()
            monitor_dict[f"train_sample_{i}_neg_count"] = neg_sample_df_i_count
            monitor_dict[f"train_sample_{i}_neg_sampling_count"] = neg_sample_df_i_sampling_count

            train_sample_i = pos_sample_df_i.unionAll(neg_sample_df_i_sampling)
            if train_sample:
                train_sample = train_sample_i.unionAll(train_sample)
            else:
                train_sample = train_sample_i
        return train_sample
    else:
        print(f"************创建测试集****************")
        test_sample = None
        for i in range(len(pos_neg_sample_list)):
            sample_df_i = spark.read.format("orc").load(pos_neg_sample_list[i])
            pos_sample_df_i_label_temp_1_count = sample_df_i.where("label_temp=1").count()
            pos_sample_df_i_label_temp_2_count = sample_df_i.where("label_temp=2").count()
            pos_sample_df_i = sample_df_i.where("label_temp=1 or label_temp=2").withColumn("label",lit(1))
            neg_sample_df_i = sample_df_i.where("label_temp=3").withColumn("label", lit(0))
            neg_sample_df_i_count = neg_sample_df_i.count()
            monitor_dict[f"test_sample_{i}_label_temp_1_count"] = pos_sample_df_i_label_temp_1_count
            monitor_dict[f"test_sample_{i}_label_temp_2_count"] = pos_sample_df_i_label_temp_2_count
            monitor_dict[f"test_sample_{i}_neg_count"] = neg_sample_df_i_count
            test_sample_i = pos_sample_df_i.unionAll(neg_sample_df_i)
            if test_sample:
                test_sample = test_sample_i.unionAll(test_sample)
            else:
                test_sample = test_sample_i
        return test_sample

def create_train_test_samples(sample_mark,model_day,train_days,test_days,train_pos_neg_sample_list,test_pos_neg_sample_list):
    model_day_date = datetime.datetime.strptime(model_day, "%Y%m%d")
    for i in range(train_days):
        sample_i_date = (model_day_date - datetime.timedelta(days=i)).strftime("%Y%m%d")
        sample_i = sample_mark.replace("SAMPLEDATE",sample_i_date)
        train_pos_neg_sample_list.append(sample_i)

    for i in range(test_days):
        sample_i_date = (model_day_date + datetime.timedelta(days=i+1)).strftime("%Y%m%d")
        sample_i = sample_mark.replace("SAMPLEDATE",sample_i_date)
        test_pos_neg_sample_list.append(sample_i)
    print("train_pos_neg_sample_list")
    print(train_pos_neg_sample_list)
    print("test_pos_neg_sample_list")
    print(test_pos_neg_sample_list)
    monitor_dict["train_pos_neg_sample_list"]=train_pos_neg_sample_list
    monitor_dict["test_pos_neg_sample_list"]=test_pos_neg_sample_list



def main():
    train_pos_neg_sample_list = []
    test_pos_neg_sample_list = []
    print("step 1：创建训练集、测试集路径")
    create_train_test_samples(sample_mark, model_day, train_days, test_days,train_pos_neg_sample_list, test_pos_neg_sample_list)
    print("step 2: 构建模型")
    bundle_app_set_indexer = CountVectorizer(inputCol="now_app_set", outputCol="now_app_set_vec",minDF=0.01)
    osversion_indexer = StringIndexer(inputCol="osversion", outputCol="osversion_index", handleInvalid="keep")
    encoder = OneHotEncoder(inputCol='osversion_index', outputCol="osversion_index_vec")
    assember = VectorAssembler(
        inputCols=["now_app_set_vec","now_app_set_size","osversion_index_vec"
            , "activeday_label_1_cnt", "activeday_label_2_cnt", "activeday_label_3_cnt", "activeday_label_4_cnt",
                   "activeday_label_5_cnt", "activeday_label_6_cnt", "activeday_label_7_cnt", "activeday_label_8_cnt",
                   "activeday_label_9_cnt", "activeday_label_10_cnt"
            , "activeday_label_1_01", "activeday_label_2_01", "activeday_label_3_01", "activeday_label_4_01",
                   "activeday_label_5_01", "activeday_label_6_01", "activeday_label_7_01", "activeday_label_8_01",
                   "activeday_label_9_01", "activeday_label_10_01"
            , "activeday_label_11_cnt", "activeday_label_12_cnt", "activeday_label_13_cnt", "activeday_label_14_cnt",
                   "activeday_label_15_cnt", "activeday_label_16_cnt", "activeday_label_17_cnt",
                   "activeday_label_18_cnt", "activeday_label_19_cnt", "activeday_label_20_cnt"
            , "activeday_label_11_01", "activeday_label_12_01", "activeday_label_13_01", "activeday_label_14_01",
                   "activeday_label_15_01", "activeday_label_16_01", "activeday_label_17_01", "activeday_label_18_01",
                   "activeday_label_19_01", "activeday_label_20_01"
            , "activeday_label_21_cnt", "activeday_label_22_cnt", "activeday_label_23_cnt", "activeday_label_24_cnt",
                   "activeday_label_25_cnt", "activeday_label_26_cnt", "activeday_label_27_cnt",
                   "activeday_label_28_cnt", "activeday_label_29_cnt", "activeday_label_30_cnt"
            , "activeday_label_21_01", "activeday_label_22_01", "activeday_label_23_01", "activeday_label_24_01",
                   "activeday_label_25_01", "activeday_label_26_01", "activeday_label_27_01", "activeday_label_28_01",
                   "activeday_label_29_01", "activeday_label_30_01"
            , "bundles_size_label"
            , "lastdaygap_label"
                   ]
        , outputCol="features"
    )
    gbt = GBTClassifier(labelCol='label', featuresCol="features", maxDepth=maxDepth, maxIter=maxIter,
                        minInfoGain=minInfoGain,
                        minInstancesPerNode=minInstancesPerNode,
                        maxBins=100)
    pipeline = Pipeline(stages=[bundle_app_set_indexer,osversion_indexer, encoder, assember, gbt])
    if model_need_only==1:
        print("******构建 has need data 模型******")
        print("step 3: 创建训练集")
        train_sample_need = create_data(train_pos_neg_sample_list, is_train=True,is_need=True)
        model_need = pipeline.fit(train_sample_need)
        model_need.write().overwrite().save(model_save_path_need)
    elif model_need_only==0:
        print("******构建 no need data 模型******")
        print("step 3: 创建训练集")
        train_sample = create_data(train_pos_neg_sample_list, is_train=True,is_need=False)
        model = pipeline.fit(train_sample)
        model.write().overwrite().save(model_save_path)
    else:
        print("******构建 has need data 模型******")
        print("step 3: 创建训练集")
        train_sample_need = create_data(train_pos_neg_sample_list, is_train=True,is_need=True)
        model_need = pipeline.fit(train_sample_need)
        model_need.write().overwrite().save(model_save_path_need)
        print("******构建 no need data 模型******")
        print("step 3: 创建训练集")
        train_sample = create_data(train_pos_neg_sample_list, is_train=True,is_need=False)
        model = pipeline.fit(train_sample)
        model.write().overwrite().save(model_save_path)



def create_test_predictions_label_bin(test_predictions,suffix=""):
    test_predictions.createTempView(f"test_predictions_table{suffix}")
    if predict_num==0:
        test_count = test_predictions.count()
    else:
        test_count = predict_num
    bin_sql = get_bin_sql(test_count)
    test_predictions_label_bin = spark.sql(f"""
        select 
            label
            ,label_temp
            ,rawPrediction_1
            ,{bin_sql} as label_bin
        from 
        (
            select 
                label
                ,label_temp
                ,rawPrediction_1
                ,row_number() over(order by rawPrediction_1 desc) as rawPrediction_1_rank
            from 
                test_predictions_table{suffix}
        ) t1
        where 
            rawPrediction_1_rank <= {test_count}
    """)
    # 统计每个桶的转化
    test_predictions_label_bin.createTempView(f"test_predictions_label_bin_table{suffix}")
    print("统计test_predictions_label_bin中各个bin的cr")
    spark.sql(f"""
        select
            label_bin
            ,count(label) as label_cnt
            
            ,count(if(label=1,1,null)) as bin_label_1_cnt
            ,round(count(if(label=1,1,null))/count(label),6) as bin_label_1_cr
            
            ,count(if(label_temp=1,1,null)) as bin_label_temp_1_cnt
            ,round(count(if(label_temp=1,1,null))/count(label),6) as bin_label_temp_1_cr
            
            ,count(if(label_temp=2,1,null)) as bin_label_temp_2_cnt
            ,round(count(if(label_temp=2,1,null))/count(label),6) as bin_label_temp_2_cr
            
            
            ,round(avg(rawPrediction_1),6)as avg_score
            ,round(max(rawPrediction_1),6) as max_score
            ,round(min(rawPrediction_1),6) as min_score
        from 
            test_predictions_label_bin_table{suffix}
        group by 
            label_bin
        order by 
            label_bin asc
    """).show(10000,truncate=False)
    return test_predictions_label_bin


def load_model_evaluate():
    print("模型评价")
    train_pos_neg_sample_list = []
    test_pos_neg_sample_list = []
    print("step 1：创建训练集、测试集路径")
    create_train_test_samples(sample_mark, model_day, train_days, test_days,train_pos_neg_sample_list, test_pos_neg_sample_list)

    print("step 2:模型评价")
    if model_need_only==1:
        print("******对 has need data 模型进行评价")
        model_need = PipelineModel.load(model_save_path_need)
        if updatemodel == 1:
            print("step 2-1：训练集评价")
            train_sample_need = create_data(train_pos_neg_sample_list, is_train=True,is_need=True)
            train_predictions_need = model_need.transform(train_sample_need).select("label",second_element("rawPrediction").alias("rawPrediction_1")).select("label","rawPrediction_1")
            eval_metric("train", train_predictions_need, "rawPrediction_1",is_need=True)
        print("step 2-2：测试集评价")
        test_sample_need = create_data(test_pos_neg_sample_list, is_train=False,is_need=True)
        test_predictions_need = model_need.transform(test_sample_need).select("label", "label_temp",second_element("rawPrediction").alias("rawPrediction_1")).select("label","label_temp","rawPrediction_1").persist()
        eval_metric("test", test_predictions_need, "rawPrediction_1",is_need=True)
        create_test_predictions_label_bin(test_predictions_need, suffix="_need")
    elif model_need_only==0:
        print("******对 no need data 模型进行评价")
        model=PipelineModel.load(model_save_path)
        print("step 2:模型评价")
        if updatemodel==1 :
            print("step 2-1：训练集评价")
            train_sample = create_data(train_pos_neg_sample_list,is_train=True,is_need=False)
            train_predictions = model.transform(train_sample).select("label",second_element("rawPrediction").alias("rawPrediction_1")).select("label","rawPrediction_1")
            eval_metric("train", train_predictions,"rawPrediction_1",is_need=False)
        print("step 2-2：测试集评价")
        test_sample= create_data(test_pos_neg_sample_list,is_train=False,is_need=False)
        test_predictions = model.transform(test_sample).select("label","label_temp",second_element("rawPrediction").alias("rawPrediction_1")).select("label","label_temp","rawPrediction_1").persist()
        eval_metric("test", test_predictions, "rawPrediction_1",is_need=False)
        create_test_predictions_label_bin(test_predictions, suffix="")
    else:
        print("******对 has need data 模型进行评价")
        model_need = PipelineModel.load(model_save_path_need)
        if updatemodel == 1:
            print("step 2-1：训练集评价")
            train_sample_need = create_data(train_pos_neg_sample_list, is_train=True,is_need=True)
            train_predictions_need = model_need.transform(train_sample_need).select("label", second_element(
                "rawPrediction").alias("rawPrediction_1")).select("label", "rawPrediction_1")
            eval_metric("train", train_predictions_need, "rawPrediction_1",is_need=True)
        print("step 2-2：测试集评价")
        test_sample_need = create_data(test_pos_neg_sample_list, is_train=False,is_need=True)
        test_predictions_need = model_need.transform(test_sample_need).select("label", "label_temp",
                                                                              second_element("rawPrediction").alias(
                                                                                  "rawPrediction_1")).select("label",
                                                                                                             "label_temp",
                                                                                                             "rawPrediction_1").persist()
        eval_metric("test", test_predictions_need, "rawPrediction_1",is_need=True)
        create_test_predictions_label_bin(test_predictions_need, suffix="_need")
        print("******对 no need data 模型进行评价")
        model = PipelineModel.load(model_save_path)
        print("step 2:模型评价")
        if updatemodel == 1:
            print("step 2-1：训练集评价")
            train_sample = create_data(train_pos_neg_sample_list, is_train=True,is_need=False)
            train_predictions = model.transform(train_sample).select("label", second_element("rawPrediction").alias(
                "rawPrediction_1")).select("label", "rawPrediction_1")
            eval_metric("train", train_predictions, "rawPrediction_1",is_need=False)
        print("step 2-2：测试集评价")
        test_sample = create_data(test_pos_neg_sample_list, is_train=False,is_need=False)
        test_predictions = model.transform(test_sample).select("label", "label_temp",
                                                               second_element("rawPrediction").alias(
                                                                   "rawPrediction_1")).select("label", "label_temp",
                                                                                              "rawPrediction_1").persist()
        eval_metric("test", test_predictions, "rawPrediction_1",is_need=False)
        create_test_predictions_label_bin(test_predictions, suffix="")




def save_output_to_oss(spark):
    sc=spark.sparkContext
    applicationId=str(sc.applicationId)
    print(f"driver applicationId {applicationId}")
    command_1=f'echo {applicationId} > driver_applicationId.log'
    os.system(command_1)
    command_2=f"hdfs dfs -put -f driver_applicationId.log  {oss_dirPath}/model/logs/"
    os.system(command_2)

def get_bin_sql(predict_count):
    sql_str = " case "
    split_float = 1/bin_num
    for i in range(bin_num):
        if i == bin_num-1:
            sql_temp = f" else {i+1} end "
            sql_str += sql_temp
        else:
            sql_temp = f" when rawPrediction_1_rank/{predict_count} <= {split_float*(i+1)} then {i+1} "
            sql_str += sql_temp
    return sql_str


def predict():
    print("针对人群库进行评分")
    predict_df = spark.read.format("orc").load(predict_dirPath)
    if model_need_only==1:
        model = PipelineModel.load(model_save_path_need)
    else:
        model = PipelineModel.load(model_save_path)
    predict_df_ini = model.transform(predict_df).select("device_id", second_element("rawPrediction").alias("rawPrediction_1")).select("device_id", "rawPrediction_1")
    predict_df_ini.createTempView("predict_df_ini_table")
    if predict_num == 0:
        predict_count = predict_df_ini.count()
    else:
        predict_count = predict_num
    monitor_dict["predict_count"]=predict_count
    bin_sql = get_bin_sql(predict_count)
    predict_df_ini_2 = spark.sql(f"""
            select
                device_id
                ,{pkg_app_index} as pkg_app_index
                ,round(rawPrediction_1,6) as rawPrediction_1
                ,{bin_sql} as predict_bin
            from
            (
                select
                    device_id
                    ,rawPrediction_1
                    ,row_number() over(order by rawPrediction_1 desc) as rawPrediction_1_rank
                from
                    predict_df_ini_table
            ) t1
            where
                rawPrediction_1_rank <= {predict_count}
            """
    ).persist()
    for i in range(1, bin_num+1):
        if model_need_only==1:
            print(f"predict has need bin {i} evaluate")
        else:
            print(f"predict no need bin {i} evaluate")
        predict_df_ini_2.where("predict_bin=%d" % i).select("rawPrediction_1").describe().show(50, truncate=False)
    predict_df_ini_2.repartition(200).write.csv(path=predict_predict_dirPath, sep=",", mode="overwrite",header=True)
    predict_df_ini_2.unpersist()
if __name__ == '__main__':
    print("logs_start")

    spark = SparkSession.builder.appName("gbdt").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    parser = argparse.ArgumentParser()

    parser.add_argument('--model_day', help='基于用户安装列表的样本构造日期')
    parser.add_argument('--days_delay', help='构建正样本的参考天数', default=1)
    parser.add_argument('--geo', help='国家')
    parser.add_argument('--platform', help='平台')
    parser.add_argument('--oss_dirname', help='oss根目录名称')
    parser.add_argument('--pkg_name', help='目标包名称,若为多个包则以&&||&&进行分割')
    parser.add_argument('--pkg_subcategory', help='目标包类别')
    parser.add_argument('--lastdaygap', help='最后活跃距今时间')


    parser.add_argument('--train_days', help='训练样本的天数', default=7)
    parser.add_argument('--test_days', help='测试样本的天数', default=1)
    parser.add_argument('--neg_pos_ratio', help='针对负样本进行采样的比例', default=20)
    parser.add_argument('--maxDepth', help='最大深度',default=5)
    parser.add_argument('--maxIter', help='模型迭代次数',default=20)
    parser.add_argument('--minInfoGain', help='最小信息增益',default=0)
    parser.add_argument('--minInstancesPerNode', help='叶子节点最小实例数',default=0)
    parser.add_argument('--predict_day', help='人群库样本')
    parser.add_argument('--predict_num', help='指定测试日期',default=0)
    parser.add_argument('--updatemodel', help='是否进行模型的更新',default=1)
    parser.add_argument('--bin_num', help='针对测试集进行分桶评价',default=50)
    parser.add_argument('--model_need_only', help='是否只选择need模型 1-needmodel 0-no need model 2-all model',default=1)
    parser.add_argument('--pkg_app_index', help='人群包的pkg_app_id')
    parser.add_argument('--label_temp_2_ratio', help='label_temp_2采样比例',default=-1)
    parser.add_argument('--model_suffix', help='模型调试时添加的后缀',default="")
    parser.add_argument('--do_predict', help='是否进行人群包的更新',default=1)
    args = parser.parse_args()
    print("args",args)
    model_day = args.model_day
    days_delay = int(args.days_delay)
    platform = args.platform
    geo = args.geo
    oss_dirname = args.oss_dirname
    pkg_name = args.pkg_name
    pkg_subcategory = args.pkg_subcategory
    lastdaygap = int(args.lastdaygap)

    train_days = int(args.train_days)
    test_days = int(args.test_days)
    neg_pos_ratio = int(args.neg_pos_ratio)
    maxDepth = int(args.maxDepth)
    maxIter = int(args.maxIter)
    minInfoGain = float(args.minInfoGain)
    minInstancesPerNode = int(args.minInstancesPerNode)
    predict_day = args.predict_day
    predict_num = int(args.predict_num)
    updatemodel = int(args.updatemodel)
    bin_num = int(args.bin_num)
    model_need_only = int(args.model_need_only)
    pkg_app_index = int(args.pkg_app_index)
    label_temp_2_ratio = int(args.label_temp_2_ratio)
    model_suffix= args.model_suffix
    do_predict= int(args.do_predict)

    oss_dirPath = f"oss://sdkemr-yeahmobi/user/chensheng/pkg/{oss_dirname}/{geo}_{platform}"
    sample_mark = f"{oss_dirPath}/sample/SAMPLEDATE_delay_{days_delay}_lastdaygap_{lastdaygap}"
    model_save_path = f"{oss_dirPath}/model/{model_day}_delay_{days_delay}_lastdaygap_{lastdaygap}{model_suffix}"
    # 将 有安装同类型app需求的用户 也当作正样本
    model_save_path_need = f"{oss_dirPath}/model_need/{model_day}_delay_{days_delay}_lastdaygap_{lastdaygap}{model_suffix}"
    predict_dirPath = f"{oss_dirPath}/predict/{predict_day}_lastdaygap_{lastdaygap}"
    predict_predict_dirPath = f"{oss_dirPath}/predict/{predict_day}_lastdaygap_{lastdaygap}_predict"
    print("predict_dirPath %s" % predict_dirPath)
    print("predict_predict_dirPath %s" % predict_predict_dirPath)
    # 添加自动化流程所需要的监控信息
    monitor_dict = {}
    monitor_dict["model_day"]=model_day
    monitor_dict["days_delay"]=days_delay
    monitor_dict["platform"]=platform
    monitor_dict["geo"]=geo
    monitor_dict["oss_dirname"]=oss_dirname
    monitor_dict["pkg_name"]=pkg_name
    monitor_dict["pkg_subcategory"]=pkg_subcategory
    monitor_dict["lastdaygap"]=lastdaygap
    monitor_dict["train_days"]=train_days
    monitor_dict["test_days"]=test_days
    monitor_dict["neg_pos_ratio"]=neg_pos_ratio
    monitor_dict["maxDepth"]=maxDepth
    monitor_dict["maxIter"]=maxIter
    monitor_dict["minInfoGain"]=minInfoGain
    monitor_dict["minInstancesPerNode"]=minInstancesPerNode
    monitor_dict["predict_day"]=predict_day
    monitor_dict["predict_num"]=predict_num
    monitor_dict["updatemodel"]=updatemodel
    monitor_dict["bin_num"]=bin_num
    monitor_dict["model_need_only"]=model_need_only
    monitor_dict["pkg_app_index"]=pkg_app_index
    monitor_dict["label_temp_2_ratio"]=label_temp_2_ratio
    monitor_dict["model_suffix"]=model_suffix
    monitor_dict["do_predict"]=do_predict
    monitor_dict["predict_dirPath"]=predict_dirPath
    monitor_dict["predict_predict_dirPath"]=predict_predict_dirPath

    #
    if updatemodel==1:
        main()
    else:
        if  model_need_only==0:
            command=f'hdfs dfs -ls -d {oss_dirPath}/model/[0-9]*_delay_{days_delay}_lastdaygap_{lastdaygap} > model_str.txt'
            os.system(command)
            f=open("model_str.txt","r")
            model_str_read=f.readlines()
            f.close()
            model_str="&&&".join(model_str_read)
            a = re.findall(f"[0-9]*_delay_{days_delay}_lastdaygap_{lastdaygap}", model_str)
            b = sorted(a, reverse=True)
            if len(b)==0:
                sys.exit()
            model_exist = b[0]
            model_save_path = f"{oss_dirPath}/model/{model_exist}"
        elif model_need_only==1:
            command = f'hdfs dfs -ls -d {oss_dirPath}/model_need/[0-9]*_delay_{days_delay}_lastdaygap_{lastdaygap}{model_suffix} > model_str.txt'
            os.system(command)
            f = open("model_str.txt", "r")
            model_str_read = f.readlines()
            f.close()
            model_str = "&&&".join(model_str_read)
            a = re.findall(f"[0-9]*_delay_{days_delay}_lastdaygap_{lastdaygap}{model_suffix}", model_str)
            b = sorted(a, reverse=True)
            if len(b) == 0:
                sys.exit()
            model_exist = b[0]
            model_save_path_need = f"{oss_dirPath}/model_need/{model_exist}"
        else:
            sys.exit()
    print(f"model_save_path {model_save_path}")
    print(f"model_save_path_need {model_save_path_need}")
    monitor_dict["model_save_path"]=model_save_path
    monitor_dict["model_save_path_need"]=model_save_path_need
    #
    load_model_evaluate()
    if do_predict:
        predict()
    print("montor_dict",monitor_dict)
    print("logs_end")
    save_output_to_oss(spark)







