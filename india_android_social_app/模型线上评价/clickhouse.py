import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.font_manager import FontManager
import numpy as np
matplotlib.rc("font",family="PingFang HK")
from matplotlib.pyplot import MultipleLocator
# mpl_fonts = set(f.name for f in FontManager().ttflist)
# print('all font list get from matplotlib.font_manager:')
# for f in sorted(mpl_fonts):
#     print('\t' + f)
#

def create_slot_label_dict(df):
    dict_temp = {}
    for i in range(len(df)):
        dict_temp[str(df.iloc[i,0])]=df.iloc[i,1]
    return dict_temp
def get_data(df_1:pd.DataFrame):
    return df_1[["label_2","click","crx1w","install"]].sort_values(axis=0,by='crx1w',ascending=False)
def get_fill(list_temp,max_len,fill_data=None):
    pad_len = max_len - len(list_temp)
    for i in range(pad_len):
        list_temp.append(fill_data)
def get_plot(offer_pkg,min_len,column_22_click,column_11_click,column_22_install,column_11_install,column_22_label_2,column_22_crx1w,column_11_crx1w):

    cr_11=np.sum([data if data else 0 for data in column_11_install])/np.sum([data if data else 0 for data in column_11_click])*10000
    cr_22=np.sum([data if data else 0 for data in column_22_install])/np.sum([data if data else 0 for data in column_22_click])*10000
    ratio = round(cr_22/cr_11,2)
    ratio_2 = round(np.mean(column_22_crx1w[:min_len])/np.mean(column_11_crx1w[:min_len]),2)

    x_major_locator = MultipleLocator(1)
    y_major_locator = MultipleLocator(0.5)
    fig,axs = plt.subplots(2,1)
    index_list = list(range(len(column_22_label_2)))
    axs[0].plot(index_list,column_22_crx1w,label='实验组')
    axs[0].plot(index_list,column_11_crx1w,label='对照组')
    axs[0].set_xlabel("cr_index")
    axs[0].set_ylabel("cr")
    axs[0].xaxis.set_major_locator(x_major_locator)
    axs[0].yaxis.set_major_locator(y_major_locator)
    axs[0].legend()
    axs[0].grid(True)

    axs[1].scatter(column_22_label_2,column_22_crx1w,label='对照组')
    axs[1].set_xlabel("model_label")
    axs[1].set_ylabel("cr")
    axs[1].legend()
    fig.suptitle(f"{offer_pkg}_{ratio_2}:1")
    fig.tight_layout()
    plt.show()

df_slot_label=pd.read_excel("/Users/ym/Desktop/工作资料/易点项目/pycharm-workspace/人群包项目/模型线上评价/slot_label.xlsx")
slot_label_dict = create_slot_label_dict(df_slot_label)
print(slot_label_dict)

file_name = "sqllab__2_20220704T030748.csv"
df_data  = pd.read_csv(f"/Users/ym/Downloads/{file_name}",header=0)
df_data["label_2"] = [slot_label_dict[str(slot)] for slot in df_data["slot"].values.tolist()]
df_data["label_3"] = [1 if str(data)[0] in ("a",'b',"c",'d','e') else 0 for data in df_data["label_2"].values.tolist()]
offer_pkg_set = set(df_data["offer_pkg"].values.tolist())
with pd.ExcelWriter("ana.xlsx") as df_writer:
    for offer_pkg in offer_pkg_set:
        df_temp = df_data[df_data.offer_pkg==offer_pkg]
        df_1 = df_temp[df_temp.label_3==1]
        df_2 = df_temp[df_temp.label_3==0]

        df_11 = get_data(df_1)
        df_22 = get_data(df_2)
        column_11_label_2 = df_11["label_2"].values.tolist()
        column_11_click = df_11["click"].values.tolist()
        column_11_crx1w = df_11["crx1w"].values.tolist()
        column_11_install = df_11["install"].values.tolist()

        column_22_label_2 = df_22["label_2"].values.tolist()
        column_22_click = df_22["click"].values.tolist()
        column_22_crx1w = df_22["crx1w"].values.tolist()
        column_22_install = df_22["install"].values.tolist()


        max_len = len(column_11_label_2) if len(column_11_label_2)>len(column_22_label_2) else len(column_22_label_2)
        min_len = len(column_11_label_2) if len(column_11_label_2)<len(column_22_label_2) else len(column_22_label_2)

        get_fill(column_11_label_2,max_len)
        get_fill(column_11_click,max_len)
        get_fill(column_11_crx1w,max_len)

        get_fill(column_22_label_2,max_len)
        get_fill(column_22_click,max_len)
        get_fill(column_22_crx1w,max_len)
        list_temp = []
        get_fill(list_temp,max_len,fill_data=offer_pkg)
        # 保存
        df = pd.DataFrame(data={"offer_pkg": list_temp, "label_1": column_11_label_2, "click_1": column_11_click,
                                "crx1w_1": column_11_crx1w, "label_2": column_22_label_2, "click_2": column_22_click,
                                "crx1w_2": column_22_crx1w})
        df.to_excel(df_writer, sheet_name=offer_pkg)
        # 绘图
        get_plot(offer_pkg,min_len,column_22_click,column_11_click,column_22_install,column_11_install,column_22_label_2, column_22_crx1w, column_11_crx1w)
        #



    


