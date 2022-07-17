"""
线上效果
offer_pkg	slot	label	device_count	install	click	crx1w	install/device_count
com.ss.android.ugc.trill	33347536	az	976750	2038	5969370	3.41	0.002086511
com.ss.android.ugc.trill	37641287	by	47039579	11121	36314593	3.06	0.000236418
com.ss.android.ugc.trill	19760998	bz	5804050	9356	31370363	2.98	0.001611978
com.ss.android.ugc.trill	44225907	ay	5405232	4029	16386409	2.46	0.000745389
com.ss.android.ugc.trill	35653768	ax	17943388	7287	30268691	2.41	0.000406111
com.ss.android.ugc.trill	78317974	cy	90292493	2453	11426423	2.15	2.71673E-05
com.ss.android.ugc.trill	57351627	aw	414875894	178	922057	1.93	4.29044E-07
com.ss.android.ugc.trill	59188090	bx	112594948	148	785713	1.88	1.31445E-06
com.ss.android.ugc.trill	35082647	cz	31685788	14893	87533739	1.7	0.000470021
"""

"""
(1)  model_1 训练时间 13小时
参数：args Namespace(bin_num=50, days_delay='2', geo='IDN', lastdaygap='7', maxDepth='15', maxIter='25', minInfoGain='0', minInstancesPerNode='50', model_day='20220707', model_need_only=1, neg_pos_ratio='10', oss_dirname='tiktok', pkg_app_index='2', pkg_name='com.ss.android.ugc.trill', pkg_subcategory='Video Players & Editors', platform='android', predict_day='20220710', predict_has_need=1, predict_num='0', test_days='1', train_days='5', updatemodel='1')
样本：lastdaygap=7 and now_app_set_size=3

predict_dirPath oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/predict/20220710_lastdaygap_7
predict_predict_dirPath oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/predict/20220710_lastdaygap_7_predict
step 1：创建训练集、测试集路径
train_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220707_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220706_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220705_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220704_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220703_delay_2_lastdaygap_7']
test_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220708_delay_2_lastdaygap_7']
step 2: 构建模型
******构建 has need data 模型******
step 3: 创建训练集
************创建训练集 has need data****************
train_sample_i:0,pos_count:476168,neg_count:120871890
train_sample_i:1,pos_count:491364,neg_count:120960922
train_sample_i:2,pos_count:490181,neg_count:120858515
train_sample_i:3,pos_count:465300,neg_count:120746716
train_sample_i:4,pos_count:443405,neg_count:120421913
model_save_path oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/model/20220707_delay_2_lastdaygap_7
model_save_path_need oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/model_need/20220707_delay_2_lastdaygap_7
模型评价
step 1：创建训练集、测试集路径
train_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220707_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220706_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220705_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220704_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220703_delay_2_lastdaygap_7']
test_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220708_delay_2_lastdaygap_7']
******对 has need data 模型进行评价
step 2:模型评价
step 2-1：训练集评价
************创建训练集 has need data****************
train_sample_i:0,pos_count:476168,neg_count:120871890
train_sample_i:1,pos_count:491364,neg_count:120960922
train_sample_i:2,pos_count:490181,neg_count:120858515
train_sample_i:3,pos_count:465300,neg_count:120746716
train_sample_i:4,pos_count:443405,neg_count:120421913
********************
train rawPrediction_1_desc
train rawPrediction_1_desc:label=1
+-------+-------------------+
|summary|    rawPrediction_1|
+-------+-------------------+
|  count|            2366418|
|   mean|-0.9020217814006651|
| stddev| 0.3486717304275537|
|    min|-1.6186890755586125|
|    max| 1.1422992414938125|
+-------+-------------------+

train rawPrediction_1_desc:label=0
+-------+-------------------+
|summary|    rawPrediction_1|
+-------+-------------------+
|  count|           23662511|
|   mean|-1.1628605758317552|
| stddev|  0.257212356382301|
|    min| -1.840053829893115|
|    max| 1.0306870206145193|
+-------+-------------------+

train AUC: 0.732363
step 2-2：测试集评价
************创建测试集 has need data****************
********************
test rawPrediction_1_desc
test rawPrediction_1_desc:label=1
+-------+-------------------+
|summary|    rawPrediction_1|
+-------+-------------------+
|  count|             447912|
|   mean|-0.9232929527166935|
| stddev| 0.3486242487205064|
|    min| -1.642100467905974|
|    max| 1.1555915910894523|
+-------+-------------------+

test rawPrediction_1_desc:label=0
+-------+-------------------+
|summary|    rawPrediction_1|
+-------+-------------------+
|  count|          120589608|
|   mean|-1.1602409862526397|
| stddev|0.25887487647731033|
|    min|-1.9837846887587711|
|    max| 1.2526796456889295|
+-------+-------------------+

test AUC: 0.711587
统计test_predictions_label_bin中各个bin的cr
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+
|label_bin|label_cnt|bin_label_1_cnt|bin_label_1_cr|bin_label_temp_1_cnt|bin_label_temp_1_cr|bin_label_temp_2_cnt|bin_label_temp_2_cr|avg_score|max_score|min_score|
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+
|1        |2420750  |49580          |0.020481      |5458                |0.002255           |44122               |0.018227           |-0.282368|1.25268  |-0.42398 |
|2        |2420750  |31861          |0.013162      |3566                |0.001473           |28295               |0.011689           |-0.50082 |-0.42398 |-0.580257|
|3        |2420751  |25063          |0.010353      |2178                |9.0E-4             |22885               |0.009454           |-0.639228|-0.580257|-0.690196|
|4        |2420750  |21151          |0.008737      |2023                |8.36E-4            |19128               |0.007902           |-0.726191|-0.690196|-0.758428|
|5        |2420751  |18787          |0.007761      |1857                |7.67E-4            |16930               |0.006994           |-0.786148|-0.758428|-0.811222|
|6        |2420750  |17230          |0.007118      |1794                |7.41E-4            |15436               |0.006377           |-0.836262|-0.811222|-0.85861 |
|7        |2420750  |15385          |0.006355      |1872                |7.73E-4            |13513               |0.005582           |-0.876771|-0.85861 |-0.889961|
|8        |2420751  |13607          |0.005621      |1075                |4.44E-4            |12532               |0.005177           |-0.90659 |-0.889961|-0.924909|
|9        |2420750  |13298          |0.005493      |944                 |3.9E-4             |12354               |0.005103           |-0.941751|-0.924909|-0.957569|
|10       |2420751  |12277          |0.005072      |647                 |2.67E-4            |11630               |0.004804           |-0.972165|-0.957569|-0.986096|
|11       |2420750  |11666          |0.004819      |680                 |2.81E-4            |10986               |0.004538           |-0.999007|-0.986096|-1.011224|
|12       |2420750  |10805          |0.004463      |640                 |2.64E-4            |10165               |0.004199           |-1.022301|-1.011224|-1.033062|
|13       |2420751  |10274          |0.004244      |499                 |2.06E-4            |9775                |0.004038           |-1.043424|-1.033062|-1.053481|
|14       |2420750  |9972           |0.004119      |498                 |2.06E-4            |9474                |0.003914           |-1.062859|-1.053481|-1.071604|
|15       |2420751  |9449           |0.003903      |476                 |1.97E-4            |8973                |0.003707           |-1.079783|-1.071604|-1.087405|
|16       |2420750  |8995           |0.003716      |624                 |2.58E-4            |8371                |0.003458           |-1.094616|-1.087405|-1.101854|
|17       |2420750  |8704           |0.003596      |437                 |1.81E-4            |8267                |0.003415           |-1.108961|-1.101854|-1.116284|
|18       |2420751  |8428           |0.003482      |381                 |1.57E-4            |8047                |0.003324           |-1.123175|-1.116284|-1.130162|
|19       |2420750  |7970           |0.003292      |439                 |1.81E-4            |7531                |0.003111           |-1.136388|-1.130162|-1.14281 |
|20       |2420751  |7683           |0.003174      |404                 |1.67E-4            |7279                |0.003007           |-1.148842|-1.14281 |-1.154893|
|21       |2420750  |7429           |0.003069      |331                 |1.37E-4            |7098                |0.002932           |-1.161104|-1.154893|-1.166755|
|22       |2420750  |7221           |0.002983      |329                 |1.36E-4            |6892                |0.002847           |-1.172816|-1.166755|-1.178577|
|23       |2420751  |6933           |0.002864      |345                 |1.43E-4            |6588                |0.002721           |-1.184303|-1.178577|-1.190078|
|24       |2420750  |6869           |0.002838      |303                 |1.25E-4            |6566                |0.002712           |-1.195837|-1.190078|-1.201585|
|25       |2420751  |6457           |0.002667      |295                 |1.22E-4            |6162                |0.002545           |-1.207212|-1.201585|-1.212709|
|26       |2420750  |6447           |0.002663      |378                 |1.56E-4            |6069                |0.002507           |-1.217782|-1.212709|-1.223174|
|27       |2420750  |6298           |0.002602      |324                 |1.34E-4            |5974                |0.002468           |-1.228353|-1.223174|-1.233456|
|28       |2420751  |6100           |0.00252       |348                 |1.44E-4            |5752                |0.002376           |-1.238495|-1.233456|-1.243601|
|29       |2420750  |5904           |0.002439      |279                 |1.15E-4            |5625                |0.002324           |-1.248734|-1.243601|-1.253803|
|30       |2420751  |5595           |0.002311      |305                 |1.26E-4            |5290                |0.002185           |-1.258418|-1.253803|-1.263344|
|31       |2420750  |5361           |0.002215      |265                 |1.09E-4            |5096                |0.002105           |-1.268176|-1.263345|-1.273127|
|32       |2420750  |5125           |0.002117      |221                 |9.1E-5             |4904                |0.002026           |-1.278106|-1.273127|-1.283094|
|33       |2420751  |4990           |0.002061      |303                 |1.25E-4            |4687                |0.001936           |-1.288045|-1.283094|-1.29272 |
|34       |2420750  |4854           |0.002005      |345                 |1.43E-4            |4509                |0.001863           |-1.297635|-1.29272 |-1.302512|
|35       |2420751  |4697           |0.00194       |335                 |1.38E-4            |4362                |0.001802           |-1.30721 |-1.302512|-1.312304|
|36       |2420750  |4472           |0.001847      |213                 |8.8E-5             |4259                |0.001759           |-1.31755 |-1.312304|-1.322831|
|37       |2420750  |4386           |0.001812      |296                 |1.22E-4            |4090                |0.00169            |-1.3278  |-1.322831|-1.332894|
|38       |2420751  |4185           |0.001729      |344                 |1.42E-4            |3841                |0.001587           |-1.338246|-1.332894|-1.343432|
|39       |2420750  |3971           |0.00164       |266                 |1.1E-4             |3705                |0.001531           |-1.34892 |-1.343432|-1.354576|
|40       |2420751  |3765           |0.001555      |279                 |1.15E-4            |3486                |0.00144            |-1.359903|-1.354576|-1.365257|
|41       |2420750  |3609           |0.001491      |326                 |1.35E-4            |3283                |0.001356           |-1.370748|-1.365257|-1.376052|
|42       |2420750  |3489           |0.001441      |338                 |1.4E-4             |3151                |0.001302           |-1.381879|-1.376052|-1.387672|
|43       |2420751  |3234           |0.001336      |303                 |1.25E-4            |2931                |0.001211           |-1.39406 |-1.387672|-1.400372|
|44       |2420750  |3026           |0.00125       |385                 |1.59E-4            |2641                |0.001091           |-1.406737|-1.400372|-1.412759|
|45       |2420751  |2844           |0.001175      |358                 |1.48E-4            |2486                |0.001027           |-1.419154|-1.412759|-1.425563|
|46       |2420750  |2611           |0.001079      |361                 |1.49E-4            |2250                |9.29E-4            |-1.432827|-1.425564|-1.440181|
|47       |2420750  |2306           |9.53E-4       |244                 |1.01E-4            |2062                |8.52E-4            |-1.449307|-1.440181|-1.45872 |
|48       |2420751  |1971           |8.14E-4       |247                 |1.02E-4            |1724                |7.12E-4            |-1.470599|-1.45872 |-1.484678|
|49       |2420750  |1434           |5.92E-4       |277                 |1.14E-4            |1157                |4.78E-4            |-1.508963|-1.484678|-1.558509|
|50       |2420751  |144            |5.9E-5        |39                  |1.6E-5             |105                 |4.3E-5             |-1.601639|-1.558509|-1.983785|
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+
（2）改进方案
label_temp_2 的预测效果较 label_temp_1 效果好，其中label_temp_2:label_temp_1=13.4
因此在进行模型训练的时候，将 label_temp_2 进行部分采样 采样比例 label_temp_2:label_temp_1=5
"""


"""
(2)  model_1 训练时间 1小时
logs_start
args Namespace(bin_num=50, days_delay='2', geo='IDN', label_temp_2_ratio='3', lastdaygap='7', maxDepth='10', maxIter='20', minInfoGain='0', minInstancesPerNode='50', model_day='20220707', model_need_only=1, model_suffix='_labelTemp2Ratio_3', neg_pos_ratio='10', oss_dirname='tiktok', pkg_app_index='2', pkg_name='com.ss.android.ugc.trill', pkg_subcategory='Video Players & Editors', platform='android', predict_day='20220710', predict_has_need=1, predict_num='0', test_days='1', train_days='5', updatemodel='1')
predict_dirPath oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/predict/20220710_lastdaygap_7
predict_predict_dirPath oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/predict/20220710_lastdaygap_7_predict
step 1：创建训练集、测试集路径
train_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220707_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220706_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220705_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220704_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220703_delay_2_lastdaygap_7']
test_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220708_delay_2_lastdaygap_7']
step 2: 构建模型
******构建 has need data 模型******
step 3: 创建训练集
************创建训练集 has need data****************
pos_sample_df_i_label_temp_1_count:36021,pos_sample_df_i_label_temp_2_count:501326,pos_sample_df_i_count:143864
train_sample_i:0,pos_count:143864,neg_count:133618575
pos_sample_df_i_label_temp_1_count:34842,pos_sample_df_i_label_temp_2_count:519146,pos_sample_df_i_count:138877
train_sample_i:1,pos_count:138877,neg_count:133737619
pos_sample_df_i_label_temp_1_count:33442,pos_sample_df_i_label_temp_2_count:519285,pos_sample_df_i_count:133669
train_sample_i:2,pos_count:133669,neg_count:133645681
pos_sample_df_i_label_temp_1_count:32894,pos_sample_df_i_label_temp_2_count:493495,pos_sample_df_i_count:131946
train_sample_i:3,pos_count:131946,neg_count:133535006
pos_sample_df_i_label_temp_1_count:32884,pos_sample_df_i_label_temp_2_count:469676,pos_sample_df_i_count:131402
train_sample_i:4,pos_count:131402,neg_count:133192732
model_save_path oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/model/20220707_delay_2_lastdaygap_7_labelTemp2Ratio_3
model_save_path_need oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/model_need/20220707_delay_2_lastdaygap_7_labelTemp2Ratio_3
模型评价
step 1：创建训练集、测试集路径
train_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220707_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220706_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220705_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220704_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220703_delay_2_lastdaygap_7']
test_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220708_delay_2_lastdaygap_7']
******对 has need data 模型进行评价
step 2:模型评价
step 2-1：训练集评价
************创建训练集 has need data****************
pos_sample_df_i_label_temp_1_count:36021,pos_sample_df_i_label_temp_2_count:501326,pos_sample_df_i_count:144242
train_sample_i:0,pos_count:144242,neg_count:133618575
pos_sample_df_i_label_temp_1_count:34842,pos_sample_df_i_label_temp_2_count:519146,pos_sample_df_i_count:139438
train_sample_i:1,pos_count:139438,neg_count:133737619
pos_sample_df_i_label_temp_1_count:33442,pos_sample_df_i_label_temp_2_count:519285,pos_sample_df_i_count:133948
train_sample_i:2,pos_count:133948,neg_count:133645681
pos_sample_df_i_label_temp_1_count:32894,pos_sample_df_i_label_temp_2_count:493495,pos_sample_df_i_count:131301
train_sample_i:3,pos_count:131301,neg_count:133535006
pos_sample_df_i_label_temp_1_count:32884,pos_sample_df_i_label_temp_2_count:469676,pos_sample_df_i_count:131205
train_sample_i:4,pos_count:131205,neg_count:133192732
********************
train rawPrediction_1_desc
train rawPrediction_1_desc:label=1
+-------+-------------------+
|summary|    rawPrediction_1|
+-------+-------------------+
|  count|             680134|
|   mean|-0.9019193344958244|
| stddev|0.33569297483646526|
|    min|  -1.53287965105745|
|    max| 0.6607372403576169|
+-------+-------------------+

train rawPrediction_1_desc:label=0
+-------+-------------------+
|summary|    rawPrediction_1|
+-------+-------------------+
|  count|            6802860|
|   mean|-1.1292682375349217|
| stddev|0.23003183244094433|
|    min|-1.6151426427366675|
|    max| 0.6422802911438202|
+-------+-------------------+

train AUC: 0.716356
step 2-2：测试集评价
************创建测试集 has need data****************
********************
test rawPrediction_1_desc
test rawPrediction_1_desc:label=1
+-------+-------------------+
|summary|    rawPrediction_1|
+-------+-------------------+
|  count|             505348|
|   mean| -0.935961414294381|
| stddev| 0.3239030974390056|
|    min|-1.5305370413871844|
|    max| 0.5676536426687587|
+-------+-------------------+

test rawPrediction_1_desc:label=0
+-------+-------------------+
|summary|    rawPrediction_1|
+-------+-------------------+
|  count|          133299938|
|   mean|-1.1268063404912154|
| stddev|0.23155250011932038|
|    min|-1.6407596603197223|
|    max| 0.8367208014899136|
+-------+-------------------+

test AUC: 0.690184
统计test_predictions_label_bin中各个bin的cr
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+
|label_bin|label_cnt|bin_label_1_cnt|bin_label_1_cr|bin_label_temp_1_cnt|bin_label_temp_1_cr|bin_label_temp_2_cnt|bin_label_temp_2_cr|avg_score|max_score|min_score|
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+
|1        |2676105  |50485          |0.018865      |7060                |0.002638           |43425               |0.016227           |-0.269834|0.836721 |-0.401372|
|2        |2676106  |33658          |0.012577      |3932                |0.001469           |29726               |0.011108           |-0.539801|-0.401372|-0.613041|
|3        |2676106  |26360          |0.00985       |3854                |0.00144            |22506               |0.00841            |-0.649459|-0.613041|-0.679134|
|4        |2676105  |20202          |0.007549      |2600                |9.72E-4            |17602               |0.006577           |-0.70292 |-0.679134|-0.730471|
|5        |2676106  |20347          |0.007603      |1957                |7.31E-4            |18390               |0.006872           |-0.766575|-0.730471|-0.796559|
|6        |2676106  |17456          |0.006523      |1208                |4.51E-4            |16248               |0.006072           |-0.825845|-0.796559|-0.854262|
|7        |2676106  |16769          |0.006266      |1123                |4.2E-4             |15646               |0.005847           |-0.878242|-0.854262|-0.899609|
|8        |2676105  |13366          |0.004995      |1233                |4.61E-4            |12133               |0.004534           |-0.914196|-0.899609|-0.926697|
|9        |2676106  |12808          |0.004786      |1062                |3.97E-4            |11746               |0.004389           |-0.942002|-0.926697|-0.953709|
|10       |2676106  |14116          |0.005275      |737                 |2.75E-4            |13379               |0.004999           |-0.968951|-0.953709|-0.983762|
|11       |2676105  |13317          |0.004976      |638                 |2.38E-4            |12679               |0.004738           |-0.99604 |-0.983762|-1.00872 |
|12       |2676106  |12728          |0.004756      |568                 |2.12E-4            |12160               |0.004544           |-1.019947|-1.00872 |-1.031239|
|13       |2676106  |12323          |0.004605      |452                 |1.69E-4            |11871               |0.004436           |-1.041446|-1.031239|-1.052206|
|14       |2676106  |11143          |0.004164      |595                 |2.22E-4            |10548               |0.003942           |-1.061157|-1.052206|-1.068438|
|15       |2676105  |11466          |0.004285      |352                 |1.32E-4            |11114               |0.004153           |-1.077144|-1.068438|-1.085467|
|16       |2676106  |11171          |0.004174      |335                 |1.25E-4            |10836               |0.004049           |-1.093469|-1.085467|-1.101267|
|17       |2676106  |10770          |0.004025      |313                 |1.17E-4            |10457               |0.003908           |-1.108295|-1.101267|-1.115141|
|18       |2676105  |10494          |0.003921      |269                 |1.01E-4            |10225               |0.003821           |-1.12163 |-1.115141|-1.127912|
|19       |2676106  |9032           |0.003375      |449                 |1.68E-4            |8583                |0.003207           |-1.132855|-1.127912|-1.137595|
|20       |2676106  |9748           |0.003643      |311                 |1.16E-4            |9437                |0.003526           |-1.142953|-1.137595|-1.148177|
|21       |2676106  |9002           |0.003364      |250                 |9.3E-5             |8752                |0.00327            |-1.153045|-1.148177|-1.157981|
|22       |2676105  |8589           |0.00321       |301                 |1.12E-4            |8288                |0.003097           |-1.162575|-1.157981|-1.16692 |
|23       |2676106  |7916           |0.002958      |424                 |1.58E-4            |7492                |0.0028             |-1.170704|-1.16692 |-1.17467 |
|24       |2676106  |8144           |0.003043      |265                 |9.9E-5             |7879                |0.002944           |-1.17898 |-1.17467 |-1.18329 |
|25       |2676106  |8000           |0.002989      |260                 |9.7E-5             |7740                |0.002892           |-1.18755 |-1.18329 |-1.191779|
|26       |2676105  |7829           |0.002926      |232                 |8.7E-5             |7597                |0.002839           |-1.195947|-1.191779|-1.200136|
|27       |2676106  |7079           |0.002645      |370                 |1.38E-4            |6709                |0.002507           |-1.203328|-1.200136|-1.207234|
|28       |2676106  |7233           |0.002703      |263                 |9.8E-5             |6970                |0.002605           |-1.211124|-1.207234|-1.215046|
|29       |2676105  |6754           |0.002524      |340                 |1.27E-4            |6414                |0.002397           |-1.218771|-1.215046|-1.221913|
|30       |2676106  |6741           |0.002519      |224                 |8.4E-5             |6517                |0.002435           |-1.225714|-1.221913|-1.22944 |
|31       |2676106  |6771           |0.00253       |264                 |9.9E-5             |6507                |0.002432           |-1.233068|-1.22944 |-1.236608|
|32       |2676106  |6477           |0.00242       |250                 |9.3E-5             |6227                |0.002327           |-1.240074|-1.236608|-1.243582|
|33       |2676105  |6237           |0.002331      |250                 |9.3E-5             |5987                |0.002237           |-1.247071|-1.243582|-1.250532|
|34       |2676106  |5989           |0.002238      |241                 |9.0E-5             |5748                |0.002148           |-1.253905|-1.250532|-1.257286|
|35       |2676106  |5876           |0.002196      |208                 |7.8E-5             |5668                |0.002118           |-1.260685|-1.257286|-1.2641  |
|36       |2676105  |5649           |0.002111      |214                 |8.0E-5             |5435                |0.002031           |-1.267484|-1.2641  |-1.270879|
|37       |2676106  |5344           |0.001997      |218                 |8.1E-5             |5126                |0.001915           |-1.274185|-1.270879|-1.277472|
|38       |2676106  |5206           |0.001945      |209                 |7.8E-5             |4997                |0.001867           |-1.280851|-1.277472|-1.284199|
|39       |2676106  |5065           |0.001893      |206                 |7.7E-5             |4859                |0.001816           |-1.287562|-1.284199|-1.290914|
|40       |2676105  |4826           |0.001803      |183                 |6.8E-5             |4643                |0.001735           |-1.294298|-1.290914|-1.297712|
|41       |2676106  |4650           |0.001738      |208                 |7.8E-5             |4442                |0.00166            |-1.301227|-1.297712|-1.304781|
|42       |2676106  |4435           |0.001657      |195                 |7.3E-5             |4240                |0.001584           |-1.30832 |-1.304781|-1.311959|
|43       |2676105  |4177           |0.001561      |231                 |8.6E-5             |3946                |0.001475           |-1.315497|-1.311959|-1.319347|
|44       |2676106  |3932           |0.001469      |204                 |7.6E-5             |3728                |0.001393           |-1.323372|-1.319347|-1.327536|
|45       |2676106  |3697           |0.001381      |185                 |6.9E-5             |3512                |0.001312           |-1.331762|-1.327536|-1.33625 |
|46       |2676106  |3295           |0.001231      |208                 |7.8E-5             |3087                |0.001154           |-1.340987|-1.33625 |-1.345673|
|47       |2676105  |3040           |0.001136      |168                 |6.3E-5             |2872                |0.001073           |-1.350714|-1.345673|-1.356079|
|48       |2676106  |2779           |0.001038      |155                 |5.8E-5             |2624                |9.81E-4            |-1.362773|-1.356079|-1.3707  |
|49       |2676106  |2329           |8.7E-4        |154                 |5.8E-5             |2175                |8.13E-4            |-1.383142|-1.3707  |-1.399391|
|50       |2676106  |528            |1.97E-4       |77                  |2.9E-5             |451                 |1.69E-4            |-1.4868  |-1.399391|-1.64076 |
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+

logs_end
"""

"""

"""

"""
(4) model_4
  logs_start
args Namespace(bin_num=50, days_delay='2', geo='IDN', label_temp_2_ratio='1', lastdaygap='7', maxDepth='5', maxIter='25', minInfoGain='0', minInstancesPerNode='20', model_day='20220707', model_need_only=1, model_suffix='_labelTemp2Ratio_1_neg_pos_ratio_20_maxIter25_maxDepth_5', neg_pos_ratio='25', oss_dirname='tiktok', pkg_app_index='2', pkg_name='com.ss.android.ugc.trill', pkg_subcategory='Video Players & Editors', platform='android', predict_day='20220710', predict_has_need=1, predict_num='0', test_days='1', train_days='5', updatemodel='1')
predict_dirPath oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/predict/20220710_lastdaygap_7
predict_predict_dirPath oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/predict/20220710_lastdaygap_7_predict
step 1：创建训练集、测试集路径
train_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220707_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220706_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220705_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220704_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220703_delay_2_lastdaygap_7']
test_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220708_delay_2_lastdaygap_7']
step 2: 构建模型
******构建 has need data 模型******
step 3: 创建训练集
************创建训练集 has need data****************
pos_sample_df_i_label_temp_1_count:36021,pos_sample_df_i_label_temp_2_count:501326,pos_sample_df_i_count:71826
train_sample_i:0,pos_count:71826,neg_count:133618575
pos_sample_df_i_label_temp_1_count:34842,pos_sample_df_i_label_temp_2_count:519146,pos_sample_df_i_count:69718
train_sample_i:1,pos_count:69718,neg_count:133737619
pos_sample_df_i_label_temp_1_count:33442,pos_sample_df_i_label_temp_2_count:519285,pos_sample_df_i_count:66677
train_sample_i:2,pos_count:66677,neg_count:133645681
pos_sample_df_i_label_temp_1_count:32894,pos_sample_df_i_label_temp_2_count:493495,pos_sample_df_i_count:65988
train_sample_i:3,pos_count:65988,neg_count:133535006
pos_sample_df_i_label_temp_1_count:32884,pos_sample_df_i_label_temp_2_count:469676,pos_sample_df_i_count:65682
train_sample_i:4,pos_count:65682,neg_count:133192732
model_save_path oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/model/20220707_delay_2_lastdaygap_7_labelTemp2Ratio_1_neg_pos_ratio_20_maxIter25_maxDepth_5
model_save_path_need oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/model_need/20220707_delay_2_lastdaygap_7_labelTemp2Ratio_1_neg_pos_ratio_20_maxIter25_maxDepth_5
模型评价
step 1：创建训练集、测试集路径
train_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220707_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220706_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220705_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220704_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220703_delay_2_lastdaygap_7']
test_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220708_delay_2_lastdaygap_7']
******对 has need data 模型进行评价
step 2:模型评价
step 2-1：训练集评价
************创建训练集 has need data****************
pos_sample_df_i_label_temp_1_count:36021,pos_sample_df_i_label_temp_2_count:501326,pos_sample_df_i_count:71814
train_sample_i:0,pos_count:71814,neg_count:133618575
pos_sample_df_i_label_temp_1_count:34842,pos_sample_df_i_label_temp_2_count:519146,pos_sample_df_i_count:69909
train_sample_i:1,pos_count:69909,neg_count:133737619
pos_sample_df_i_label_temp_1_count:33442,pos_sample_df_i_label_temp_2_count:519285,pos_sample_df_i_count:66867
train_sample_i:2,pos_count:66867,neg_count:133645681
pos_sample_df_i_label_temp_1_count:32894,pos_sample_df_i_label_temp_2_count:493495,pos_sample_df_i_count:65774
train_sample_i:3,pos_count:65774,neg_count:133535006
pos_sample_df_i_label_temp_1_count:32884,pos_sample_df_i_label_temp_2_count:469676,pos_sample_df_i_count:65956
train_sample_i:4,pos_count:65956,neg_count:133192732
********************
train rawPrediction_1_desc
train rawPrediction_1_desc:label=1
+-------+--------------------+
|summary|     rawPrediction_1|
+-------+--------------------+
|  count|              340320|
|   mean| -1.2084666067878156|
| stddev|  0.2634984948833026|
|    min|  -1.571473805579165|
|    max|-0.44501442330048746|
+-------+--------------------+

train rawPrediction_1_desc:label=0
+-------+--------------------+
|summary|     rawPrediction_1|
+-------+--------------------+
|  count|             8506147|
|   mean| -1.3988891115417728|
| stddev| 0.16952835032678676|
|    min| -1.6389559212544182|
|    max|-0.44501442330048746|
+-------+--------------------+

train AUC: 0.731290
step 2-2：测试集评价
************创建测试集 has need data****************
********************
test rawPrediction_1_desc
test rawPrediction_1_desc:label=1
+-------+--------------------+
|summary|     rawPrediction_1|
+-------+--------------------+
|  count|              505348|
|   mean| -1.2892973409704584|
| stddev|  0.2421090958148305|
|    min|  -1.600740619738218|
|    max|-0.44501442330048746|
+-------+--------------------+

test rawPrediction_1_desc:label=0
+-------+-------------------+
|summary|    rawPrediction_1|
+-------+-------------------+
|  count|          133299938|
|   mean| -1.397007690718926|
| stddev| 0.1702924535162927|
|    min|-1.6441741679142166|
|    max|-0.4425364386408236|
+-------+-------------------+

test AUC: 0.643873
统计test_predictions_label_bin中各个bin的cr
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+
|label_bin|label_cnt|bin_label_1_cnt|bin_label_1_cr|bin_label_temp_1_cnt|bin_label_temp_1_cr|bin_label_temp_2_cnt|bin_label_temp_2_cr|avg_score|max_score|min_score|
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+
|1        |2676105  |44825          |0.01675       |6830                |0.002552           |37995               |0.014198           |-0.774618|-0.442536|-0.896395|
|2        |2676106  |24848          |0.009285      |4324                |0.001616           |20524               |0.007669           |-0.957085|-0.896395|-0.986228|
|3        |2676106  |26525          |0.009912      |3882                |0.001451           |22643               |0.008461           |-0.999498|-0.986228|-1.026656|
|4        |2676105  |17384          |0.006496      |2224                |8.31E-4            |15160               |0.005665           |-1.066653|-1.026656|-1.098925|
|5        |2676106  |14213          |0.005311      |2245                |8.39E-4            |11968               |0.004472           |-1.114395|-1.098925|-1.129441|
|6        |2676106  |15156          |0.005663      |1475                |5.51E-4            |13681               |0.005112           |-1.146066|-1.129441|-1.170996|
|7        |2676106  |15009          |0.005609      |1658                |6.2E-4             |13351               |0.004989           |-1.187787|-1.170996|-1.200176|
|8        |2676105  |14832          |0.005542      |895                 |3.34E-4            |13937               |0.005208           |-1.221031|-1.200176|-1.242094|
|9        |2676106  |11493          |0.004295      |1184                |4.42E-4            |10309               |0.003852           |-1.252961|-1.242094|-1.263433|
|10       |2676106  |12282          |0.00459       |1093                |4.08E-4            |11189               |0.004181           |-1.279337|-1.263433|-1.289591|
|11       |2676105  |10281          |0.003842      |1089                |4.07E-4            |9192                |0.003435           |-1.293708|-1.289591|-1.304428|
|12       |2676106  |12628          |0.004719      |623                 |2.33E-4            |12005               |0.004486           |-1.318118|-1.304428|-1.333509|
|13       |2676106  |11115          |0.004153      |659                 |2.46E-4            |10456               |0.003907           |-1.351467|-1.333509|-1.365302|
|14       |2676106  |11869          |0.004435      |576                 |2.15E-4            |11293               |0.00422            |-1.380581|-1.365302|-1.388836|
|15       |2676105  |11139          |0.004162      |469                 |1.75E-4            |10670               |0.003987           |-1.399282|-1.388836|-1.404748|
|16       |2676106  |10587          |0.003956      |412                 |1.54E-4            |10175               |0.003802           |-1.411229|-1.404748|-1.416907|
|17       |2676106  |9976           |0.003728      |394                 |1.47E-4            |9582                |0.003581           |-1.418975|-1.416907|-1.421301|
|18       |2676105  |10564          |0.003948      |352                 |1.32E-4            |10212               |0.003816           |-1.429327|-1.421301|-1.436392|
|19       |2676106  |10349          |0.003867      |262                 |9.8E-5             |10087               |0.003769           |-1.441472|-1.436392|-1.444231|
|20       |2676106  |9128           |0.003411      |285                 |1.06E-4            |8843                |0.003304           |-1.446478|-1.444231|-1.44916 |
|21       |2676106  |7572           |0.002829      |402                 |1.5E-4             |7170                |0.002679           |-1.450946|-1.44916 |-1.452046|
|22       |2676105  |9113           |0.003405      |236                 |8.8E-5             |8877                |0.003317           |-1.455754|-1.452046|-1.459669|
|23       |2676106  |8929           |0.003337      |237                 |8.9E-5             |8692                |0.003248           |-1.463534|-1.459669|-1.465915|
|24       |2676106  |8509           |0.00318       |378                 |1.41E-4            |8131                |0.003038           |-1.467836|-1.465915|-1.469599|
|25       |2676106  |8492           |0.003173      |218                 |8.1E-5             |8274                |0.003092           |-1.471346|-1.469599|-1.472923|
|26       |2676105  |7742           |0.002893      |272                 |1.02E-4            |7470                |0.002791           |-1.474725|-1.472923|-1.476905|
|27       |2676106  |8085           |0.003021      |219                 |8.2E-5             |7866                |0.002939           |-1.478532|-1.476905|-1.480413|
|28       |2676106  |7662           |0.002863      |168                 |6.3E-5             |7494                |0.0028             |-1.481593|-1.480413|-1.482333|
|29       |2676105  |8264           |0.003088      |210                 |7.8E-5             |8054                |0.00301            |-1.483824|-1.482333|-1.485613|
|30       |2676106  |7525           |0.002812      |154                 |5.8E-5             |7371                |0.002754           |-1.486158|-1.485613|-1.487598|
|31       |2676106  |8661           |0.003236      |182                 |6.8E-5             |8479                |0.003168           |-1.489196|-1.487598|-1.490552|
|32       |2676106  |8347           |0.003119      |208                 |7.8E-5             |8139                |0.003041           |-1.491755|-1.490552|-1.492934|
|33       |2676105  |7856           |0.002936      |198                 |7.4E-5             |7658                |0.002862           |-1.494058|-1.492934|-1.495113|
|34       |2676106  |7204           |0.002692      |156                 |5.8E-5             |7048                |0.002634           |-1.496081|-1.495113|-1.497019|
|35       |2676106  |6799           |0.002541      |174                 |6.5E-5             |6625                |0.002476           |-1.497996|-1.497019|-1.498937|
|36       |2676105  |6795           |0.002539      |152                 |5.7E-5             |6643                |0.002482           |-1.499843|-1.498937|-1.500899|
|37       |2676106  |6284           |0.002348      |148                 |5.5E-5             |6136                |0.002293           |-1.501738|-1.500899|-1.502754|
|38       |2676106  |6170           |0.002306      |140                 |5.2E-5             |6030                |0.002253           |-1.503651|-1.502754|-1.504601|
|39       |2676106  |5540           |0.00207       |143                 |5.3E-5             |5397                |0.002017           |-1.505517|-1.504601|-1.506497|
|40       |2676105  |5771           |0.002156      |148                 |5.5E-5             |5623                |0.002101           |-1.507578|-1.506497|-1.508817|
|41       |2676106  |5366           |0.002005      |134                 |5.0E-5             |5232                |0.001955           |-1.509875|-1.508817|-1.511064|
|42       |2676106  |5435           |0.002031      |117                 |4.4E-5             |5318                |0.001987           |-1.512237|-1.511064|-1.51359 |
|43       |2676105  |5794           |0.002165      |98                  |3.7E-5             |5696                |0.002128           |-1.515068|-1.51359 |-1.516629|
|44       |2676106  |5389           |0.002014      |81                  |3.0E-5             |5308                |0.001983           |-1.517917|-1.516629|-1.519484|
|45       |2676106  |5942           |0.00222       |104                 |3.9E-5             |5838                |0.002182           |-1.52136 |-1.519484|-1.523401|
|46       |2676106  |5289           |0.001976      |69                  |2.6E-5             |5220                |0.001951           |-1.525206|-1.523401|-1.526595|
|47       |2676105  |5489           |0.002051      |73                  |2.7E-5             |5416                |0.002024           |-1.528491|-1.526595|-1.530297|
|48       |2676106  |4691           |0.001753      |86                  |3.2E-5             |4605                |0.001721           |-1.531927|-1.530297|-1.533495|
|49       |2676106  |4046           |0.001512      |88                  |3.3E-5             |3958                |0.001479           |-1.535354|-1.533495|-1.537616|
|50       |2676106  |2384           |8.91E-4       |81                  |3.0E-5             |2303                |8.61E-4            |-1.540878|-1.537616|-1.644174|
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+

logs_end
"""


"""
(5) model need & no need 
logs_start
args Namespace(bin_num=50, days_delay='2', geo='IDN', label_temp_2_ratio='1', lastdaygap='7', maxDepth='10', maxIter='10', minInfoGain='0', minInstancesPerNode='200', model_day='20220707', model_need_only='0', model_suffix='_labelTemp2Ratio1_neg_pos_ratio20_maxIter10_maxDepth10', neg_pos_ratio='20', oss_dirname='tiktok', pkg_app_index='2', pkg_name='com.ss.android.ugc.trill', pkg_subcategory='Video Players & Editors', platform='android', predict_day='20220710', predict_has_need=1, predict_num='0', test_days='1', train_days='5', updatemodel='1')
predict_dirPath oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/predict/20220710_lastdaygap_7
predict_predict_dirPath oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/predict/20220710_lastdaygap_7_predict
step 1：创建训练集、测试集路径
train_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220707_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220706_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220705_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220704_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220703_delay_2_lastdaygap_7']
test_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220708_delay_2_lastdaygap_7']
step 2: 构建模型
******构建 has need data 模型******
step 3: 创建训练集
************创建训练集 has need data****************
pos_sample_df_i_label_temp_1_count:36021,pos_sample_df_i_label_temp_2_count:501326,pos_sample_df_i_count:72095
train_sample_i:0,pos_count:72095,neg_count:133618575
pos_sample_df_i_label_temp_1_count:34842,pos_sample_df_i_label_temp_2_count:519146,pos_sample_df_i_count:69552
train_sample_i:1,pos_count:69552,neg_count:133737619
pos_sample_df_i_label_temp_1_count:33442,pos_sample_df_i_label_temp_2_count:519285,pos_sample_df_i_count:66930
train_sample_i:2,pos_count:66930,neg_count:133645681
pos_sample_df_i_label_temp_1_count:32894,pos_sample_df_i_label_temp_2_count:493495,pos_sample_df_i_count:65800
train_sample_i:3,pos_count:65800,neg_count:133535006
pos_sample_df_i_label_temp_1_count:32884,pos_sample_df_i_label_temp_2_count:469676,pos_sample_df_i_count:65609
train_sample_i:4,pos_count:65609,neg_count:133192732
******构建 no need data 模型******
step 3: 创建训练集
************创建训练集 no need data****************
train_sample_i:0,pos_count:36021,neg_count:133618575
train_sample_i:1,pos_count:34842,neg_count:133737619
train_sample_i:2,pos_count:33442,neg_count:133645681
train_sample_i:3,pos_count:32894,neg_count:133535006
train_sample_i:4,pos_count:32884,neg_count:133192732
model_save_path oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/model/20220707_delay_2_lastdaygap_7_labelTemp2Ratio1_neg_pos_ratio20_maxIter10_maxDepth10
model_save_path_need oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/model_need/20220707_delay_2_lastdaygap_7_labelTemp2Ratio1_neg_pos_ratio20_maxIter10_maxDepth10
模型评价
step 1：创建训练集、测试集路径
train_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220707_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220706_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220705_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220704_delay_2_lastdaygap_7', 'oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220703_delay_2_lastdaygap_7']
test_pos_neg_sample_list
['oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample/20220708_delay_2_lastdaygap_7']
******对 has need data 模型进行评价
step 2:模型评价
step 2-1：训练集评价
************创建训练集 has need data****************
pos_sample_df_i_label_temp_1_count:36021,pos_sample_df_i_label_temp_2_count:501326,pos_sample_df_i_count:71780
train_sample_i:0,pos_count:71780,neg_count:133618575
pos_sample_df_i_label_temp_1_count:34842,pos_sample_df_i_label_temp_2_count:519146,pos_sample_df_i_count:69745
train_sample_i:1,pos_count:69745,neg_count:133737619
pos_sample_df_i_label_temp_1_count:33442,pos_sample_df_i_label_temp_2_count:519285,pos_sample_df_i_count:66913
train_sample_i:2,pos_count:66913,neg_count:133645681
pos_sample_df_i_label_temp_1_count:32894,pos_sample_df_i_label_temp_2_count:493495,pos_sample_df_i_count:66065
train_sample_i:3,pos_count:66065,neg_count:133535006
pos_sample_df_i_label_temp_1_count:32884,pos_sample_df_i_label_temp_2_count:469676,pos_sample_df_i_count:66046
train_sample_i:4,pos_count:66046,neg_count:133192732
********************
train rawPrediction_1_desc
train rawPrediction_1_desc:label=1
+-------+--------------------+
|summary|     rawPrediction_1|
+-------+--------------------+
|  count|              340549|
|   mean| -0.9795348826691064|
| stddev|   0.251779041299986|
|    min| -1.3646686641323826|
|    max|-0.05745296121908418|
+-------+--------------------+

train rawPrediction_1_desc:label=0
+-------+--------------------+
|summary|     rawPrediction_1|
+-------+--------------------+
|  count|             6805438|
|   mean| -1.1615788747412203|
| stddev| 0.14581291528022894|
|    min| -1.3862386813915788|
|    max|-0.05745296121908418|
+-------+--------------------+

train AUC: 0.745951 label_temp=1 and label_temp=2
train AUC: 0.838137 label_temp=1
step 2-2：测试集评价
************创建测试集 has need data****************
********************
test rawPrediction_1_desc
test rawPrediction_1_desc:label=1
+-------+--------------------+
|summary|     rawPrediction_1|
+-------+--------------------+
|  count|              505348|
|   mean| -1.0558748055798504|
| stddev| 0.22291453014126336|
|    min| -1.3682268471816506|
|    max|-0.05745296121908418|
+-------+--------------------+

test rawPrediction_1_desc:label=0
+-------+--------------------+
|summary|     rawPrediction_1|
+-------+--------------------+
|  count|           133299938|
|   mean|  -1.160161532829603|
| stddev| 0.14671850312921517|
|    min|   -1.39073301983158|
|    max|-0.01436420469899...|
+-------+--------------------+

test AUC: 0.663015 label_temp=1 and label_temp=2
test AUC: 0.832008 label_temp=1
统计test_predictions_label_bin中各个bin的cr
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+
|label_bin|label_cnt|bin_label_1_cnt|bin_label_1_cr|bin_label_temp_1_cnt|bin_label_temp_1_cr|bin_label_temp_2_cnt|bin_label_temp_2_cr|avg_score|max_score|min_score|
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+
|1        |2676105  |47041          |0.017578      |7345                |0.002745           |39696               |0.014833           |-0.55143 |-0.014364|-0.678506|
|2        |2676106  |27032          |0.010101      |5640                |0.002108           |21392               |0.007994           |-0.754052|-0.678513|-0.77784 |
|3        |2676106  |23897          |0.00893       |2894                |0.001081           |21003               |0.007848           |-0.805278|-0.77784 |-0.854494|
|4        |2676105  |22762          |0.008506      |2665                |9.96E-4            |20097               |0.00751            |-0.89629 |-0.854494|-0.924306|
|5        |2676106  |15438          |0.005769      |1830                |6.84E-4            |13608               |0.005085           |-0.942587|-0.924306|-0.964012|
|6        |2676106  |14943          |0.005584      |1524                |5.69E-4            |13419               |0.005014           |-0.982508|-0.964012|-1.003721|
|7        |2676106  |14655          |0.005476      |1398                |5.22E-4            |13257               |0.004954           |-1.019515|-1.003721|-1.033285|
|8        |2676105  |15047          |0.005623      |1170                |4.37E-4            |13877               |0.005186           |-1.046675|-1.033285|-1.059115|
|9        |2676106  |14252          |0.005326      |899                 |3.36E-4            |13353               |0.00499            |-1.07049 |-1.059115|-1.081963|
|10       |2676106  |11319          |0.00423       |920                 |3.44E-4            |10399               |0.003886           |-1.089382|-1.081963|-1.098645|
|11       |2676105  |11496          |0.004296      |838                 |3.13E-4            |10658               |0.003983           |-1.10611 |-1.098645|-1.114468|
|12       |2676106  |12413          |0.004638      |657                 |2.46E-4            |11756               |0.004393           |-1.123909|-1.114468|-1.134174|
|13       |2676106  |10022          |0.003745      |722                 |2.7E-4             |9300                |0.003475           |-1.140907|-1.134174|-1.145992|
|14       |2676106  |12511          |0.004675      |477                 |1.78E-4            |12034               |0.004497           |-1.152832|-1.145992|-1.159032|
|15       |2676105  |11728          |0.004382      |432                 |1.61E-4            |11296               |0.004221           |-1.16362 |-1.159032|-1.168801|
|16       |2676106  |11540          |0.004312      |347                 |1.3E-4             |11193               |0.004183           |-1.173297|-1.168801|-1.177427|
|17       |2676106  |10726          |0.004008      |350                 |1.31E-4            |10376               |0.003877           |-1.18078 |-1.177427|-1.184194|
|18       |2676105  |10491          |0.00392       |313                 |1.17E-4            |10178               |0.003803           |-1.187426|-1.184194|-1.190484|
|19       |2676106  |9606           |0.00359       |354                 |1.32E-4            |9252                |0.003457           |-1.192887|-1.190484|-1.194779|
|20       |2676106  |8885           |0.00332       |343                 |1.28E-4            |8542                |0.003192           |-1.196561|-1.194779|-1.198684|
|21       |2676106  |9640           |0.003602      |301                 |1.12E-4            |9339                |0.00349            |-1.200892|-1.198684|-1.202529|
|22       |2676105  |9611           |0.003591      |246                 |9.2E-5             |9365                |0.003499           |-1.204286|-1.202529|-1.206103|
|23       |2676106  |9038           |0.003377      |231                 |8.6E-5             |8807                |0.003291           |-1.207671|-1.206103|-1.209024|
|24       |2676106  |9082           |0.003394      |233                 |8.7E-5             |8849                |0.003307           |-1.210683|-1.209024|-1.212202|
|25       |2676106  |8736           |0.003264      |229                 |8.6E-5             |8507                |0.003179           |-1.21363 |-1.212202|-1.214958|
|26       |2676105  |7625           |0.002849      |284                 |1.06E-4            |7341                |0.002743           |-1.216129|-1.214958|-1.2172  |
|27       |2676106  |8309           |0.003105      |210                 |7.8E-5             |8099                |0.003026           |-1.218572|-1.2172  |-1.219918|
|28       |2676106  |7898           |0.002951      |218                 |8.1E-5             |7680                |0.00287            |-1.221189|-1.219918|-1.222397|
|29       |2676105  |7387           |0.00276       |188                 |7.0E-5             |7199                |0.00269            |-1.223671|-1.222397|-1.224827|
|30       |2676106  |7405           |0.002767      |183                 |6.8E-5             |7222                |0.002699           |-1.226037|-1.224827|-1.227227|
|31       |2676106  |7160           |0.002676      |170                 |6.4E-5             |6990                |0.002612           |-1.22847 |-1.227227|-1.229753|
|32       |2676106  |7186           |0.002685      |183                 |6.8E-5             |7003                |0.002617           |-1.230937|-1.229753|-1.232024|
|33       |2676105  |6861           |0.002564      |156                 |5.8E-5             |6705                |0.002506           |-1.233195|-1.232024|-1.234409|
|34       |2676106  |6632           |0.002478      |155                 |5.8E-5             |6477                |0.00242            |-1.235658|-1.234409|-1.236949|
|35       |2676106  |6570           |0.002455      |146                 |5.5E-5             |6424                |0.002401           |-1.238193|-1.236949|-1.239506|
|36       |2676105  |6321           |0.002362      |148                 |5.5E-5             |6173                |0.002307           |-1.240691|-1.239506|-1.241913|
|37       |2676106  |6388           |0.002387      |141                 |5.3E-5             |6247                |0.002334           |-1.243228|-1.241913|-1.244511|
|38       |2676106  |6123           |0.002288      |141                 |5.3E-5             |5982                |0.002235           |-1.245665|-1.244511|-1.246991|
|39       |2676106  |5866           |0.002192      |119                 |4.4E-5             |5747                |0.002148           |-1.248085|-1.246991|-1.249269|
|40       |2676105  |5594           |0.00209       |124                 |4.6E-5             |5470                |0.002044           |-1.250379|-1.249269|-1.251402|
|41       |2676106  |5386           |0.002013      |129                 |4.8E-5             |5257                |0.001964           |-1.252397|-1.251402|-1.253503|
|42       |2676106  |5217           |0.001949      |127                 |4.7E-5             |5090                |0.001902           |-1.254559|-1.253503|-1.255751|
|43       |2676105  |4901           |0.001831      |127                 |4.7E-5             |4774                |0.001784           |-1.257081|-1.255751|-1.25854 |
|44       |2676106  |4681           |0.001749      |121                 |4.5E-5             |4560                |0.001704           |-1.260002|-1.25854 |-1.261515|
|45       |2676106  |4153           |0.001552      |109                 |4.1E-5             |4044                |0.001511           |-1.262938|-1.261515|-1.264507|
|46       |2676106  |4091           |0.001529      |123                 |4.6E-5             |3968                |0.001483           |-1.266093|-1.264507|-1.267599|
|47       |2676105  |3818           |0.001427      |100                 |3.7E-5             |3718                |0.001389           |-1.269309|-1.267599|-1.271127|
|48       |2676106  |3703           |0.001384      |105                 |3.9E-5             |3598                |0.001344           |-1.273349|-1.271127|-1.275789|
|49       |2676106  |3230           |0.001207      |80                  |3.0E-5             |3150                |0.001177           |-1.278816|-1.275789|-1.282381|
|50       |2676106  |932            |3.48E-4       |60                  |2.2E-5             |872                 |3.26E-4            |-1.300042|-1.282381|-1.390733|
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+

******对 no need data 模型进行评价
step 2:模型评价
step 2-1：训练集评价
************创建训练集 no need data****************
train_sample_i:0,pos_count:36021,neg_count:133618575
train_sample_i:1,pos_count:34842,neg_count:133737619
train_sample_i:2,pos_count:33442,neg_count:133645681
train_sample_i:3,pos_count:32894,neg_count:133535006
train_sample_i:4,pos_count:32884,neg_count:133192732
********************
train rawPrediction_1_desc
train rawPrediction_1_desc:label=1
+-------+-------------------+
|summary|    rawPrediction_1|
+-------+-------------------+
|  count|             170083|
|   mean|-0.7867644479827607|
| stddev| 0.3379348254546127|
|    min|-1.3841656122389001|
|    max| 0.3408657697995052|
+-------+-------------------+

train rawPrediction_1_desc:label=0
+-------+-------------------+
|summary|    rawPrediction_1|
+-------+-------------------+
|  count|            3398004|
|   mean|-1.1804725228244382|
| stddev|0.20258883406196582|
|    min|-1.3841656122389001|
|    max|0.30573550422501256|
+-------+-------------------+

train AUC: 0.859159 label_temp=1 and label_temp=2
train AUC: 0.859159 label_temp=1
step 2-2：测试集评价
************创建测试集 no need data:%d****************
********************
test rawPrediction_1_desc
test rawPrediction_1_desc:label=1
+-------+-------------------+
|summary|    rawPrediction_1|
+-------+-------------------+
|  count|             505348|
|   mean|-1.0728723265677167|
| stddev| 0.2994470589971565|
|    min|-1.3584089762637843|
|    max| 0.3408657697995052|
+-------+-------------------+

test rawPrediction_1_desc:label=0
+-------+-------------------+
|summary|    rawPrediction_1|
+-------+-------------------+
|  count|          133299938|
|   mean|-1.1785211315664086|
| stddev|0.20381117235524773|
|    min|-1.3841656122389001|
|    max|0.36702027849068475|
+-------+-------------------+

test AUC: 0.596704 label_temp=1 and label_temp=2
test AUC: 0.852895 label_temp=1
统计test_predictions_label_bin中各个bin的cr
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+
|label_bin|label_cnt|bin_label_1_cnt|bin_label_1_cr|bin_label_temp_1_cnt|bin_label_temp_1_cr|bin_label_temp_2_cnt|bin_label_temp_2_cr|avg_score|max_score|min_score|
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+
|1        |2676105  |36694          |0.013712      |8105                |0.003029           |28589               |0.010683           |-0.383224|0.36702  |-0.550759|
|2        |2676106  |25270          |0.009443      |4857                |0.001815           |20413               |0.007628           |-0.599471|-0.550759|-0.64498 |
|3        |2676106  |24650          |0.009211      |3603                |0.001346           |21047               |0.007865           |-0.703536|-0.64498 |-0.75447 |
|4        |2676105  |16540          |0.006181      |2314                |8.65E-4            |14226               |0.005316           |-0.775417|-0.75447 |-0.790813|
|5        |2676106  |15296          |0.005716      |2098                |7.84E-4            |13198               |0.004932           |-0.827754|-0.790813|-0.863689|
|6        |2676106  |15358          |0.005739      |1601                |5.98E-4            |13757               |0.005141           |-0.899381|-0.863689|-0.937467|
|7        |2676106  |11105          |0.00415       |1488                |5.56E-4            |9617                |0.003594           |-0.966881|-0.937467|-0.985344|
|8        |2676105  |9765           |0.003649      |1170                |4.37E-4            |8595                |0.003212           |-1.002877|-0.985344|-1.018009|
|9        |2676106  |12265          |0.004583      |1175                |4.39E-4            |11090               |0.004144           |-1.043083|-1.018009|-1.065742|
|10       |2676106  |12105          |0.004523      |927                 |3.46E-4            |11178               |0.004177           |-1.086483|-1.065742|-1.105317|
|11       |2676105  |10994          |0.004108      |787                 |2.94E-4            |10207               |0.003814           |-1.11929 |-1.105317|-1.130456|
|12       |2676106  |10637          |0.003975      |712                 |2.66E-4            |9925                |0.003709           |-1.143493|-1.130456|-1.155273|
|13       |2676106  |8824           |0.003297      |614                 |2.29E-4            |8210                |0.003068           |-1.1625  |-1.155273|-1.170636|
|14       |2676106  |8495           |0.003174      |515                 |1.92E-4            |7980                |0.002982           |-1.177246|-1.170636|-1.183794|
|15       |2676105  |9710           |0.003628      |506                 |1.89E-4            |9204                |0.003439           |-1.190889|-1.183794|-1.198465|
|16       |2676106  |8443           |0.003155      |432                 |1.61E-4            |8011                |0.002994           |-1.204071|-1.198465|-1.209004|
|17       |2676106  |8754           |0.003271      |390                 |1.46E-4            |8364                |0.003125           |-1.214878|-1.209004|-1.219703|
|18       |2676105  |7048           |0.002634      |394                 |1.47E-4            |6654                |0.002486           |-1.223301|-1.219703|-1.226956|
|19       |2676106  |7667           |0.002865      |313                 |1.17E-4            |7354                |0.002748           |-1.230982|-1.226956|-1.234173|
|20       |2676106  |8432           |0.003151      |354                 |1.32E-4            |8078                |0.003019           |-1.237716|-1.234173|-1.241577|
|21       |2676106  |8477           |0.003168      |251                 |9.4E-5             |8226                |0.003074           |-1.244963|-1.241577|-1.248205|
|22       |2676105  |7962           |0.002975      |236                 |8.8E-5             |7726                |0.002887           |-1.25153 |-1.248205|-1.254713|
|23       |2676106  |7280           |0.00272       |199                 |7.4E-5             |7081                |0.002646           |-1.257631|-1.254713|-1.26003 |
|24       |2676106  |7701           |0.002878      |208                 |7.8E-5             |7493                |0.0028             |-1.262258|-1.26003 |-1.26492 |
|25       |2676106  |9673           |0.003615      |196                 |7.3E-5             |9477                |0.003541           |-1.267737|-1.26492 |-1.270173|
|26       |2676105  |6019           |0.002249      |198                 |7.4E-5             |5821                |0.002175           |-1.270738|-1.270173|-1.271755|
|27       |2676106  |9258           |0.00346       |194                 |7.2E-5             |9064                |0.003387           |-1.273702|-1.271755|-1.275627|
|28       |2676106  |9062           |0.003386      |178                 |6.7E-5             |8884                |0.00332            |-1.277371|-1.275627|-1.279344|
|29       |2676105  |8584           |0.003208      |140                 |5.2E-5             |8444                |0.003155           |-1.281018|-1.279344|-1.282306|
|30       |2676106  |6838           |0.002555      |129                 |4.8E-5             |6709                |0.002507           |-1.282997|-1.282306|-1.28378 |
|31       |2676106  |9906           |0.003702      |156                 |5.8E-5             |9750                |0.003643           |-1.285115|-1.28378 |-1.28639 |
|32       |2676106  |9283           |0.003469      |160                 |6.0E-5             |9123                |0.003409           |-1.287431|-1.28639 |-1.288693|
|33       |2676105  |9559           |0.003572      |126                 |4.7E-5             |9433                |0.003525           |-1.289777|-1.288693|-1.290938|
|34       |2676106  |7517           |0.002809      |142                 |5.3E-5             |7375                |0.002756           |-1.291886|-1.290938|-1.292373|
|35       |2676106  |8794           |0.003286      |122                 |4.6E-5             |8672                |0.003241           |-1.293345|-1.292373|-1.294353|
|36       |2676105  |7913           |0.002957      |86                  |3.2E-5             |7827                |0.002925           |-1.295417|-1.294353|-1.296186|
|37       |2676106  |6454           |0.002412      |124                 |4.6E-5             |6330                |0.002365           |-1.296506|-1.296186|-1.297391|
|38       |2676106  |8254           |0.003084      |91                  |3.4E-5             |8163                |0.00305            |-1.29785 |-1.297391|-1.298774|
|39       |2676106  |9293           |0.003473      |84                  |3.1E-5             |9209                |0.003441           |-1.299693|-1.298774|-1.300485|
|40       |2676105  |6654           |0.002486      |68                  |2.5E-5             |6586                |0.002461           |-1.301353|-1.300485|-1.301677|
|41       |2676106  |8573           |0.003204      |93                  |3.5E-5             |8480                |0.003169           |-1.302663|-1.301677|-1.304607|
|42       |2676106  |5948           |0.002223      |80                  |3.0E-5             |5868                |0.002193           |-1.30546 |-1.304607|-1.305602|
|43       |2676105  |7494           |0.0028        |66                  |2.5E-5             |7428                |0.002776           |-1.307033|-1.305602|-1.308205|
|44       |2676106  |5955           |0.002225      |63                  |2.4E-5             |5892                |0.002202           |-1.308617|-1.308205|-1.308727|
|45       |2676106  |8323           |0.00311       |67                  |2.5E-5             |8256                |0.003085           |-1.309399|-1.308727|-1.310187|
|46       |2676106  |7516           |0.002809      |56                  |2.1E-5             |7460                |0.002788           |-1.31068 |-1.310187|-1.311168|
|47       |2676105  |6213           |0.002322      |37                  |1.4E-5             |6176                |0.002308           |-1.311723|-1.311168|-1.312287|
|48       |2676106  |8734           |0.003264      |40                  |1.5E-5             |8694                |0.003249           |-1.314018|-1.312287|-1.31675 |
|49       |2676106  |7031           |0.002627      |39                  |1.5E-5             |6992                |0.002613           |-1.317267|-1.31675 |-1.317614|
|50       |2676106  |7028           |0.002626      |21                  |8.0E-6             |7007                |0.002618           |-1.318457|-1.317614|-1.384166|
+---------+---------+---------------+--------------+--------------------+-------------------+--------------------+-------------------+---------+---------+---------+


"""