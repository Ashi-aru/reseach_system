from pathlib import Path
from datetime import date
# 自分で定義した関数・クラスをimport
from datafact_model import Datafact
from logging_config import setup_logger
from debug import debug_datafact


PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/"data"
TODAY = date.today().strftime("%Y-%m-%d")

logger = setup_logger()

FUNC_D = {
    1:"合計値を計算(sum)",
    2:"合計値の割合を計算(sum_percent)",
    3:"平均値を計算(mean)",
    # 4:"平均値の割合を計算(mean_percent)",
    5:"最大値を計算(max)",
    6:"最小値を計算(min)",
    7:"中央値を計算(meadian)",
    8:"データ数をカウント(count)",
    9:"データ数の割合をカウント(count_percent)",
    10:"ユニークな値の数をカウント(nunique)",
    11:"ユニークな値を列挙(unique)"
    }
F_NUM2F_NAME_D = {
    1:"sum",
    2:"sum_percent",
    3:"mean",
    4:"mean_percent",
    5:"max",
    6:"min",
    7:"meadian",
    8:"count",
    9:"count_percent",
    10:"nunique",
    11:"unique"
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
            choices = "\n".join([f"{k}:{v}" for k, v in FUNC_D.items() if(k<=9)])
            user_choice = [int(n) for n in input(instruction+"\n"+choices+"\n").split(",")]
        else:
            choices = "\n".join([f"{k}:{v}" for k, v in FUNC_D.items() if(k>=8)])
            user_choice = [int(n) for n in input(instruction+"\n"+choices+"\n").split(",")]
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
            if(func_num<11): # 時系列データにのみSclarArithmeticを実施
                return_d[(focus_att,func_num)]=[2,4] # 数字の意味についてはOPERATOR_Dを参照
            else:
                return_d[(focus_att,func_num)]=[None]
    return return_d


"""
subject、被Aggregation属性、Aggregation_f, Operatorを受け取り、datafact.operationのリストを出力する関数

入力
- agg_attrs: 被Aggregation属性のリスト。
- agg_f_d: define_aggregation_Fの出力。
- operator_d: define_scalar_arithmetic_operatorの出力
- subject: datafact.subject。src/make_subject.pyで生成
- ordinal_d: （時系列の）Ordinal属性について、順序を保存した辞書.({"年":[2019,2020,...,2024],..})
- step_n: 1/2/3。
    - 1: Aggregation、Aggregation→ScalarArithmeticを実行
    - 2: Aggregation→Rank、Aggregation→ScalarArithmetic→Rankを実行
    - 3: Aggregation→Rank→ScalarArithmeticを実行
出力
- operation_l: datafact.operationのリスト。
"""
def make_operations(agg_attrs, agg_f_d, operator_d, subject, ordinal_d, step_n, attr_type):
    """
    Scalar Arithmeticが可能であるかどうか判定する
    """
    def can_ScalarArithmetic(agg_attr, f_num, col_name):
        if((operator_d[(agg_attr, f_num)]==[None]) or (attr_type[col_name]!='Ordinal_t')):
            return False
        n = ordinal_d[col_name].index(filter_value[0])
        if(n==len(ordinal_d[col_name])-1): # NOTE: ordinal_dは降順を想定しているが、昇順の方が良いのか、、？？
            return False
        return True

    operation_l = []
    parents, col_name, filter_value = subject
    # logger.info(f'agg_attrs={agg_attrs}, agg_f_d={agg_f_d},\noperator_d={operator_d}, ordinal_d={ordinal_d}\nsubject={subject}')
    # NOTE: subject=="root"時の対応だけ別途用意、、。
    if(subject == [{},"",[]]):
        for agg_attr in agg_attrs:
            for f_num in agg_f_d[agg_attr]:
                operation_agg = ["Aggregation", agg_attr, F_NUM2F_NAME_D[f_num]]
                operation_l.append(operation_agg)
        return operation_l

    if(step_n==1):
        for agg_attr in agg_attrs:
            for f_num in agg_f_d[agg_attr]:
                # Aggregation
                operation_agg = ["Aggregation", agg_attr, F_NUM2F_NAME_D[f_num]]
                operation_l.append(operation_agg)

                # Aggregation→Scalar
                if(not can_ScalarArithmetic(agg_attr, f_num, col_name)):
                    continue
                n = ordinal_d[col_name].index(filter_value[0])
                subject1, subject2 = subject, [parents,col_name,[ordinal_d[col_name][n+1]]]
                for op in operator_d[(agg_attr, f_num)]:
                    if(op is None): continue
                    operation_scalar = [
                        "ScalarArithmetic", 
                        OPERATOR_D[op], 
                        Datafact(subject1, operation_agg),
                        Datafact(subject2, operation_agg)
                    ]
                    operation_l.append(operation_scalar)
    if(step_n==2):
        for agg_attr in agg_attrs:
            for f_num in agg_f_d[agg_attr]:
                # Aggregation→Rank
                if(f_num>=11): continue
                operation_agg = ["Aggregation", agg_attr, F_NUM2F_NAME_D[f_num]]
                subject_ = [parents, col_name, ["*"]]
                operation_agg_rank = ["Rank", "降順", Datafact(subject_, operation_agg)]
                operation_l.append(operation_agg_rank)

                # Aggregation→Scalar→Rank
                if(not can_ScalarArithmetic(agg_attr, f_num, col_name)):
                    continue
                n = ordinal_d[col_name].index(filter_value[0])
                subject1, subject2 = [parents, col_name, ["n"]], [parents, col_name, ["n-1"]]
                for op in operator_d[(agg_attr, f_num)]:
                    if(op is None): continue
                    operation_scalar = [
                        "ScalarArithmetic", 
                        OPERATOR_D[op], 
                        Datafact(subject1, operation_agg),
                        Datafact(subject2, operation_agg)
                    ]
                    operation_scalar_rank = [
                        "Rank",
                        "降順",
                        Datafact([parents, col_name, ["*"]], operation_scalar)
                    ]
                    operation_l.append(operation_scalar_rank)
    if(step_n==3):
        for agg_attr in agg_attrs:
            for f_num in agg_f_d[agg_attr]:
                if(f_num>=11): continue
                operation_agg = ["Aggregation", agg_attr, F_NUM2F_NAME_D[f_num]]
                operation_agg_rank = ["Rank", "降順", Datafact([parents, col_name, ["*"]], operation_agg)]
                if(not can_ScalarArithmetic(agg_attr, f_num, col_name)):
                    continue
                n = ordinal_d[col_name].index(filter_value[0])
                subject1, subject2 = subject, [parents,col_name,[ordinal_d[col_name][n+1]]]
                for op in operator_d[(agg_attr, f_num)]:
                    if(op is None): continue
                    operation_agg_rank_scalar = [
                        "ScalarArithmetic", 
                        OPERATOR_D[op], 
                        Datafact(subject1, operation_agg_rank),
                        Datafact(subject2, operation_agg_rank)
                    ]
                    # logger.info(f'make_operation.py:\n{operation_agg_rank_scalar}')
                    operation_l.append(operation_agg_rank_scalar)
    return operation_l
    

"""
datafacts用に、datafact.operationのリストを出力する関数

入力
- agg_attrs: 被Aggregation属性のリスト。
- agg_f_d: define_aggregation_Fの出力。
- operator_d: define_scalar_arithmetic_operatorの出力
- subject: datafact.subject。src/make_subject.pyで生成
- ordinal_d: （時系列の）Ordinal属性について、順序を保存した辞書.({"年":[2019,2020,...,2024],..})
出力
- operation_l: datafact.operationのリスト。
"""
def make_operations_for_datafacts(agg_attrs, agg_f_d, operator_d, subject, ordinal_d, attr_type):
    """
    Scalar Arithmeticが可能であるかどうか判定する
    """
    def can_ScalarArithmetic(agg_attr, f_num, col_name):
        if((operator_d[(agg_attr, f_num)]==[None]) or (attr_type[col_name]!='Ordinal_t')):
            return False
        # n = ordinal_d[col_name].index(filter_value[0])
        # if(n==len(ordinal_d[col_name])-1): # NOTE: ordinal_dは降順を想定しているが、昇順の方が良いのか、、？？
        #     return False
        return True
    
    """
    ScalarArithmetic部分を作る関数
    """
    def make_ScalarArithmetic(operation, operation_l):
        if(filter_value==["*"]):
            subject1, subject2 = [parents, col_name, ["n"]], [parents, col_name, ["n-1"]]
        else:
            n = ordinal_d[col_name].index(filter_value[0])
            subject1, subject2 = [parents, col_name, filter_value], [parents, col_name, [ordinal_d[col_name][n+1]]]
        for op in operator_d[(agg_attr, f_num)]:
            if(op is None): continue
            operation_scalar = [
                "ScalarArithmetic", 
                OPERATOR_D[op], 
                Datafact(subject1, operation),
                Datafact(subject2, operation)
            ]
            operation_l.append(operation_scalar)
        return operation_l

    operation_l = []
    parents, col_name, filter_value = subject
    
    # Aggregation, Aggregation→Scalar
    for agg_attr in agg_attrs:
        for f_num in agg_f_d[agg_attr]:
            # Aggregation
            operation_agg = ["Aggregation", agg_attr, F_NUM2F_NAME_D[f_num]]
            operation_l.append(operation_agg)

            # Aggregation→Scalar
            if(not can_ScalarArithmetic(agg_attr, f_num, col_name)):
                continue
            operation_l = make_ScalarArithmetic(operation_agg, operation_l)

    # Aggregation→Rank→Scalar
    # for agg_attr in agg_attrs:
    #     for f_num in agg_f_d[agg_attr]:
    #         if(f_num>=11): continue
    #         operation_agg = ["Aggregation", agg_attr, F_NUM2F_NAME_D[f_num]]
    #         operation_agg_rank = ["Rank", "降順", Datafact([parents, col_name, ["*"]], operation_agg)]
    #         if(not can_ScalarArithmetic(agg_attr, f_num, col_name)):
    #             continue
    #         operation_l = make_ScalarArithmetic(operation_agg_rank, operation_l)
    return operation_l

