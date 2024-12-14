from pathlib import Path
from datetime import date
# 自分で定義した関数・クラスをimport
from logging_config import setup_logger


PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/"data"
TODAY = date.today().strftime("%Y-%m-%d")

logger = setup_logger()

FUNC_D = {
    1:"合計値を計算(sum)",
    2:"合計値の割合を計算(sum_percent)",
    3:"平均値を計算(mean)",
    4:"平均値の割合を計算(mean_percent)",
    5:"最大値を計算(max)",
    6:"最小値を計算(min)",
    7:"中央値を計算(meadian)",
    8:"データ数をカウント(count)",
    9:"データ数の割合をカウント(count_percent)",
    10:"ユニークな値の数をカウント(nunique)",
    11:"ユニークな値を列挙(unique)"
    }

OPERATOR_D = {
    1:"+",
    2:"-",
    3:"*",
    4:"/",
}


"""
注目属性ごとにAggregation関数を選択する処理の実装

入力
- att_l: ["売上","従業員数","上場flg"]
- att_label_d: {"売上":"Quantitative","上場flg":"Categorical",...})
出力
- {"売上":[1,3,5],"従業員数":[1,3,5],"上場flg":[2]}
"""
def define_aggregation_F(att_l, att_label_d):
    return_d = {}
    for att_name in att_l:
        instruction = f"「{att_name}」属性に適応したい操作を以下から選択し、その数字をカンマ(,)区切りで入力してください"
        if(att_label_d[att_name]=="Quantitative"):
            choices = "\n".join([f"{k}:{v}" for k, v in FUNC_D.items() if(k<=6)])
            user_choice = [int(n) for n in input(instruction+"\n\n"+choices+"\n").split(",")]
        else:
            choices = "\n".join([f"{k}:{v}" for k, v in FUNC_D.items() if(k>=6)])
            user_choice = [int(n) for n in input(instruction+"\n\n"+choices+"\n").split(",")]
        return_d[att_name] = user_choice
    return return_d


"""
注目属性の属性タイプ(CA,QA,OA)・Aggregation関数の組み合わせごとにScalarArithmeticの演算子を決定する処理の実装

入力:
- focus_att_l: 注目属性(計算を行う属性)のリスト, ["売上","従業員数"]
- focus_att2func_d: define_aggregation_Fの出力, {"売上":[1,3,4],"従業員数":[1,3,4],"上場flg":[9]}
- attr_type_d: 各属性のタイプ(Categoricalなど)を保存した辞書
出力: 
- {(注目属性, Aggregation_F):演算子, ...}
"""
def define_scalar_arithmetic_operator(focus_att_l, focus_att2func_d, attr_type_d):
    return_d = {}
    for focus_att in focus_att_l:
        for func_num in focus_att2func_d[focus_att]:
            if(func_num<10 and attr_type_d[focus_att]=="Ordinal_t"): # 時系列データにのみSclarArithmeticを実施
                return_d[(focus_att,func_num)]=[2,4] # 数字の意味についてはOPERATOR_Dを参照
            else:
                return_d[(focus_att,func_num)]=[None]
    return return_d
