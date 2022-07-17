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
def eval_metric(data_type,prediction,prediction_col="rawPrediction_1"):
    print("*"*20)
    print("%s %s_desc"%(data_type,prediction_col))
    print("%s %s_desc:label=1" % (data_type,prediction_col))
    prediction.select(prediction_col).where("label=1").describe().show()
    print("%s %s_desc:label=0" % (data_type,prediction_col))
    prediction.select(prediction_col).where("label=0").describe().show()
    evaluator = BinaryClassificationEvaluator(rawPredictionCol=prediction_col)
    print('%s AUC: %.6f' % (data_type, evaluator.evaluate(prediction)))


def create_date(pos_neg_sample_list,is_train=True):
    if is_train:
        print("************创建训练集****************")
        train_sample = None
        for i in range(len(pos_neg_sample_list)):
            pos_sample_df_i = spark.read.format("orc").load(pos_neg_sample_list[i][0]).drop("target_app_label").withColumn('label',lit(1))
            pos_sample_df_i_count = pos_sample_df_i.count()
            neg_sample_df_i = spark.read.format("orc").load(pos_neg_sample_list[i][1]).withColumn('label',lit(0))
            neg_sample_df_i_count = neg_sample_df_i.count()
            neg_sample_df_i_sampling = neg_sample_df_i.sample(pos_sample_df_i_count*neg_pos_ratio/neg_sample_df_i_count)
            train_sample_i = pos_sample_df_i.unionAll(neg_sample_df_i_sampling)
            if train_sample:
                train_sample = train_sample_i.unionAll(train_sample)
            else:
                train_sample = train_sample_i
            print("train_sample_i:%d,train_sample_count:%d" % (i, train_sample.count()))
        # train_sample_shuffle = train_sample.withColumn('shuffle_rand_index',rand())
        # train_sample_shuffle_order = train_sample_shuffle.orderBy(train_sample_shuffle.shuffle_rand_index)
        # train_sample_end = train_sample_shuffle_order.drop(train_sample_shuffle.shuffle_rand_index)
        return train_sample
    else:
        print("************创建测试集: %d****************")
        test_sample = None
        for i in range(len(pos_neg_sample_list)):
            pos_sample_df_i = spark.read.format("orc").load(pos_neg_sample_list[i][0]).drop("target_app_label").withColumn('label', lit(1))
            neg_sample_df_i = spark.read.format("orc").load(pos_neg_sample_list[i][1]).withColumn('label', lit(0))
            test_sample_i = pos_sample_df_i.unionAll(neg_sample_df_i)
            if test_sample:
                test_sample = test_sample_i.unionAll(test_sample)
            else:
                test_sample = test_sample_i
            print("test_sample_i:%d,test_sample_count:%d" % (i, test_sample.count()))
        return test_sample



def create_train_test_samples(pos_sample_mark,neg_sample_mark,model_day,train_days,test_days,train_pos_neg_sample_list,test_pos_neg_sample_list,test_day_sure):
    model_day_date = datetime.datetime.strptime(model_day, "%Y%m%d")
    for i in range(train_days):
        sample_i = (model_day_date - datetime.timedelta(days=i)).strftime("%Y%m%d")
        pos_sample_i = pos_sample_mark.replace("SAMPLEDATE",sample_i)
        neg_sample_i = neg_sample_mark.replace("SAMPLEDATE",sample_i)
        train_pos_neg_sample_list.append((pos_sample_i,neg_sample_i))
    if test_day_sure:
        sample_i = test_day_sure
        pos_sample_i = pos_sample_mark.replace("SAMPLEDATE", sample_i)
        neg_sample_i = neg_sample_mark.replace("SAMPLEDATE", sample_i)
        test_pos_neg_sample_list.append((pos_sample_i, neg_sample_i))
    else:
        for i in range(test_days):
            sample_i = (model_day_date + datetime.timedelta(days=i+1)).strftime("%Y%m%d")
            pos_sample_i = pos_sample_mark.replace("SAMPLEDATE", sample_i)
            neg_sample_i = neg_sample_mark.replace("SAMPLEDATE", sample_i)
            test_pos_neg_sample_list.append((pos_sample_i, neg_sample_i))
    print("train_pos_neg_sample_list")
    print(train_pos_neg_sample_list)
    print("test_pos_neg_sample_list")
    print(test_pos_neg_sample_list)


def main():
    train_pos_neg_sample_list = []
    test_pos_neg_sample_list = []
    print("step 1：创建训练集、测试集路径")
    create_train_test_samples(pos_sample_mark, neg_sample_mark, model_day, train_days, test_days,train_pos_neg_sample_list, test_pos_neg_sample_list,test_day_sure)
    print("step 2: 创建训练集、测试集")
    train_sample = create_date(train_pos_neg_sample_list,is_train=True)
    print("step 3: 构建模型")
    # "device_id", "osversion", "target_app_label"
    # , "bundle_social_set", "bundle_social_set_size"
    # , "activeday_label_1_cnt", "activeday_label_2_cnt", "activeday_label_3_cnt", "activeday_label_4_cnt", "activeday_label_5_cnt", "activeday_label_6_cnt", "activeday_label_7_cnt", "activeday_label_8_cnt", "activeday_label_9_cnt", "activeday_label_10_cnt"
    # , "activeday_label_1_01", "activeday_label_2_01", "activeday_label_3_01", "activeday_label_4_01", "activeday_label_5_01", "activeday_label_6_01", "activeday_label_7_01", "activeday_label_8_01", "activeday_label_9_01", "activeday_label_10_01"
    # , "activeday_label_11_cnt", "activeday_label_12_cnt", "activeday_label_13_cnt", "activeday_label_14_cnt", "activeday_label_15_cnt", "activeday_label_16_cnt", "activeday_label_17_cnt", "activeday_label_18_cnt", "activeday_label_19_cnt", "activeday_label_20_cnt"
    # , "activeday_label_11_01", "activeday_label_12_01", "activeday_label_13_01", "activeday_label_14_01", "activeday_label_15_01", "activeday_label_16_01", "activeday_label_17_01", "activeday_label_18_01", "activeday_label_19_01", "activeday_label_20_01"
    # , "activeday_label_21_cnt", "activeday_label_22_cnt", "activeday_label_23_cnt", "activeday_label_24_cnt", "activeday_label_25_cnt", "activeday_label_26_cnt", "activeday_label_27_cnt", "activeday_label_28_cnt", "activeday_label_29_cnt", "activeday_label_30_cnt"
    # , "activeday_label_21_01", "activeday_label_22_01", "activeday_label_23_01", "activeday_label_24_01", "activeday_label_25_01", "activeday_label_26_01", "activeday_label_27_01", "activeday_label_28_01", "activeday_label_29_01", "activeday_label_30_01"
    # , "bundles_size_label"
    # , "lastdaygap_label"
    bundle_social_set_indexer = CountVectorizer(inputCol="bundle_social_set", outputCol="bundle_social_set_vec",minDF=0.01)
    osversion_indexer = StringIndexer(inputCol="osversion", outputCol="osversion_index", handleInvalid="keep")
    # discretizer = QuantileDiscretizer(numBuckets=20, inputCol="bundle_social_set_size", outputCol="bundle_social_set_size_label")
    encoder = OneHotEncoder(inputCol='osversion_index', outputCol="osversion_index_vec")
    assember = VectorAssembler(
        inputCols=["osversion_index_vec", "bundle_social_set_vec", "bundle_social_set_size"
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
    pipeline = Pipeline(stages=[bundle_social_set_indexer, osversion_indexer, encoder, assember, gbt])
    model = pipeline.fit(train_sample)
    model.write().overwrite().save(model_save_path)




def create_test_predictions_label_bin(test_predictions):
    test_predictions.createTempView("test_predictions_table")
    if predict_num==0:
        test_count = test_predictions.count()
    else:
        test_count = predict_num
    test_predictions_label_bin = spark.sql(f"""
        select 
            label
            ,rawPrediction_1
            ,case
            when rawPrediction_1_rank/{test_count} <=0.05 then 1 
            when rawPrediction_1_rank/{test_count} <=0.10 then 2
            when rawPrediction_1_rank/{test_count} <=0.15 then 3
            when rawPrediction_1_rank/{test_count} <=0.20 then 4
            when rawPrediction_1_rank/{test_count} <=0.25 then 5
            when rawPrediction_1_rank/{test_count} <=0.30 then 6
            when rawPrediction_1_rank/{test_count} <=0.35 then 7
            when rawPrediction_1_rank/{test_count} <=0.40 then 8
            when rawPrediction_1_rank/{test_count} <=0.45 then 9
            when rawPrediction_1_rank/{test_count} <=0.50 then 10
            when rawPrediction_1_rank/{test_count} <=0.55 then 11
            when rawPrediction_1_rank/{test_count} <=0.60 then 12
            when rawPrediction_1_rank/{test_count} <=0.65 then 13
            when rawPrediction_1_rank/{test_count} <=0.70 then 14
            when rawPrediction_1_rank/{test_count} <=0.75 then 15
            when rawPrediction_1_rank/{test_count} <=0.80 then 16
            when rawPrediction_1_rank/{test_count} <=0.85 then 17
            when rawPrediction_1_rank/{test_count} <=0.90 then 18
            when rawPrediction_1_rank/{test_count} <=0.95 then 19
            else 20
            end as label_bin
        from 
        (
            select 
                label
                ,rawPrediction_1
                ,row_number() over(order by rawPrediction_1 desc) as rawPrediction_1_rank
            from 
                test_predictions_table
        ) t1
        where 
            rawPrediction_1_rank <= {test_count}
    """)
    # 统计每个桶的转化
    test_predictions_label_bin.createTempView("test_predictions_label_bin_table")
    print("统计test_predictions_label_bin中各个bin的cr")
    spark.sql("""
        select
            label_bin
            ,count(label) as label_cnt
            ,count(if(label=1,1,null)) as bin_label_1_cnt
            ,round(count(if(label=1,1,null))/count(label),6) as bin_cr
            ,round(avg(rawPrediction_1),6)as avg_score
            ,round(max(rawPrediction_1),6) as max_score
            ,round(min(rawPrediction_1),6) as min_score
        from 
            test_predictions_label_bin_table
        group by 
            label_bin
        order by 
            label_bin asc
    """).show(50,truncate=False)
    return test_predictions_label_bin

def load_model_evaluate():
    model=PipelineModel.load(model_save_path)
    train_pos_neg_sample_list = []
    test_pos_neg_sample_list = []
    print("step 1：创建训练集、测试集路径")
    create_train_test_samples(pos_sample_mark, neg_sample_mark, model_day, train_days, test_days,train_pos_neg_sample_list, test_pos_neg_sample_list,test_day_sure)
    # evaluation
    print("step 4:模型评价")
    if updatemodel==1:
        print("step 4-1：训练集评价")
        train_sample = create_date(train_pos_neg_sample_list,is_train=True)
        train_predictions = model.transform(train_sample).select("label",second_element("rawPrediction").alias("rawPrediction_1")).select("label","rawPrediction_1")
        eval_metric("train", train_predictions,"rawPrediction_1")
    print("step 4-2：测试集评价")
    test_sample= create_date(test_pos_neg_sample_list,is_train=False)
    test_predictions = model.transform(test_sample).select("label",second_element("rawPrediction").alias("rawPrediction_1")).select("label","rawPrediction_1").persist()
    eval_metric("test", test_predictions, "rawPrediction_1")

    # 将test_sample 按照分数由高到低进行等分并进行评价
    test_predictions_label_bin = create_test_predictions_label_bin(test_predictions)
    test_predictions_label_bin.persist()
    test_predictions.unpersist()
    for i in range(1,21):
        df_temp = test_predictions_label_bin.where("label_bin=%d"%i).select("label","rawPrediction_1")
        eval_metric("test_%d"%i,df_temp,"rawPrediction_1")
    test_predictions_label_bin.unpersist()

    # 针对人群库进行评分
    print("针对人群库进行评分")
    candidata_df = spark.read.format("orc").load(candidate_dirPath)
    predict_df_ini = model.transform(candidata_df).select("device_id",second_element("rawPrediction").alias("rawPrediction_1")).select("device_id", "rawPrediction_1").persist()
    predict_df_ini.createTempView("predict_df_ini_table")
    if predict_num==0:
        predict_count = predict_df_ini.count()
    else:
        predict_count= predict_num
    predict_df_ini_2 = spark.sql(f"""
        select
            device_id
            ,{pkg_app_index} as pkg_app_index
            ,round(rawPrediction_1,6) as rawPrediction_1
            ,case
            when rawPrediction_1_rank/{predict_count} <=0.05 then 1
            when rawPrediction_1_rank/{predict_count} <=0.10 then 2
            when rawPrediction_1_rank/{predict_count} <=0.15 then 3
            when rawPrediction_1_rank/{predict_count} <=0.20 then 4
            when rawPrediction_1_rank/{predict_count} <=0.25 then 5
            when rawPrediction_1_rank/{predict_count} <=0.30 then 6
            when rawPrediction_1_rank/{predict_count} <=0.35 then 7
            when rawPrediction_1_rank/{predict_count} <=0.40 then 8
            when rawPrediction_1_rank/{predict_count} <=0.45 then 9
            when rawPrediction_1_rank/{predict_count} <=0.50 then 10
            when rawPrediction_1_rank/{predict_count} <=0.55 then 11
            when rawPrediction_1_rank/{predict_count} <=0.60 then 12
            when rawPrediction_1_rank/{predict_count} <=0.65 then 13
            when rawPrediction_1_rank/{predict_count} <=0.70 then 14
            when rawPrediction_1_rank/{predict_count} <=0.75 then 15
            when rawPrediction_1_rank/{predict_count} <=0.80 then 16
            when rawPrediction_1_rank/{predict_count} <=0.85 then 17
            when rawPrediction_1_rank/{predict_count} <=0.90 then 18
            when rawPrediction_1_rank/{predict_count} <=0.95 then 19
            else 20
            end as predict_bin
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
    )
    predict_df_ini_2.repartition(200).write.csv(path="%s_csv"%candidate_predict_dirPath,sep=",",mode="overwrite",header=True)
    predict_df_ini.unpersist()
    print("**********predict_df 分桶describe**********")
    for i in range(1, 21):
        predict_df_ini_2.where("predict_bin=%d" % i).select("rawPrediction_1").describe().show(50,truncate=False)
def save_output_to_oss(spark):
    sc=spark.sparkContext
    applicationId=str(sc.applicationId)
    print(f"driver applicationId {applicationId}")
    command_1=f'echo {applicationId} > driver_applicationId.log'
    os.system(command_1)
    command_2=f"hdfs dfs -put -f driver_applicationId.log  oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app/logs/"
    os.system(command_2)



if __name__ == '__main__':
    print("logs_start")
    spark = SparkSession.builder.appName("gbdt").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_day',help='模型的日期',default='20220623')
    parser.add_argument('--train_days', help='训练样本的天数', default=7)
    parser.add_argument('--test_days', help='测试样本的天数', default=1)
    parser.add_argument('--neg_pos_ratio', help='针对负样本进行采样的比例', default=20)
    parser.add_argument('--maxDepth', help='最大深度',default=30)
    parser.add_argument('--maxIter', help='模型迭代次数',default=10)
    parser.add_argument('--minInfoGain', help='最小信息增益',default=0.001)
    parser.add_argument('--minInstancesPerNode', help='叶子节点最小实例数',default=50)
    parser.add_argument('--model_index', help='叶子节点最小实例数',default=0)
    parser.add_argument('--candidate_day', help='人群库样本')
    parser.add_argument('--candidate_predict_index', help='0',default=0)
    parser.add_argument('--test_day_sure', help='指定测试日期',default=None)
    parser.add_argument('--predict_num', help='指定测试日期',default=0)
    parser.add_argument('--lastActivedays', help='用户最后一天活跃',default=3)
    parser.add_argument('--updatemodel', help='是否进行模型的更新',default=1)
    parser.add_argument('--model_list', help='若不进行模型更新,则',default=None)
    parser.add_argument('--pkg_app_index', help='若不进行模型更新,则')

    args = parser.parse_args()
    print("args",args)
    model_day = args.model_day
    train_days = int(args.train_days)
    test_days = int(args.test_days)
    neg_pos_ratio = int(args.neg_pos_ratio)
    maxDepth = int(args.maxDepth)
    maxIter = int(args.maxIter)
    minInfoGain = float(args.minInfoGain)
    minInstancesPerNode = int(args.minInstancesPerNode)
    model_index = int(args.model_index)
    candidate_day = args.candidate_day
    candidate_predict_index = int(args.candidate_predict_index)
    test_day_sure = args.test_day_sure
    predict_num = int(args.predict_num)
    lastActivedays = int(args.lastActivedays)
    updatemodel = int(args.updatemodel)
    pkg_app_index = int(args.pkg_app_index)


    if lastActivedays == 3:
        sample_suffix = "_delay"
        candidate_suffix=""
    else:
        sample_suffix = "_delay_%d" % lastActivedays
        candidate_suffix = "_%d" % lastActivedays

    sample_dirPath = "oss://sdkemr-yeahmobi/user/chensheng/sample_create_social_app"
    pos_sample_mark = "%s/oss_SAMPLEDATE_pos_sample%s_2" % (sample_dirPath,sample_suffix)
    neg_sample_mark = "%s/oss_SAMPLEDATE_neg_sample%s_2" % (sample_dirPath,sample_suffix)
    model_save_path = "%s/model_%s_%d" % (sample_dirPath,model_day,model_index)
    candidate_dirPath = "%s/oss_%s_candidate%s_2"%(sample_dirPath,candidate_day,candidate_suffix)
    candidate_predict_dirPath = "%s/oss_%s_candidate_predict_%d" % (sample_dirPath, candidate_day , candidate_predict_index)
    print("candidate_dirPath %s" % candidate_dirPath)
    print("candidate_predict_dirPath %s" % candidate_predict_dirPath)
    if updatemodel==1:
        main()
    else:
        command='hdfs dfs -ls -d %s/model_[0-9]*_%d > model_str.txt'%(sample_dirPath,model_index)
        os.system(command)
        f=open("model_str.txt","r")
        model_str_read=f.readlines()
        f.close()
        model_str="&&&".join(model_str_read)
        a = re.findall("model_[0-9]*_%d" % model_index, model_str)
        b = sorted(a, reverse=True)
        if len(b)==0:
            sys.exit()
        model_exist = b[0]
        model_save_path = "%s/%s" % (sample_dirPath, model_exist)
    print(f"model_save_path {model_save_path}")
    load_model_evaluate()
    print("logs_end")
    save_output_to_oss(spark)







