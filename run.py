import sys
from pathlib import Path
import pandas as pd
import time
from datetime import datetime
sys.path.append('/Users/ashikawaharuki/Desktop/research/TDB/test/system/src')
# 自分で作成した関数・クラスをimport
from logging_config import setup_logger
from dataframe_metainfo import DataFrameMetaInfo
from datafact_manager import DatafactManager
from drilldown import drilldown
from cal_significance import cal_subtree_significance
from cal_datafact import cal_subtree_nodes
from section import Section


PROJ_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJ_DIR/"data"

logger = setup_logger()


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

if(__name__ == '__main__'):
    # NOTE: ここはユーザーからの入力を受け取る形にする
    table = '_amazon-purchases.csv'
    df = pd.read_csv(DATA_DIR/f'tables/{table}')
    sample_df = df.head(2)
    df_description = "2018年から2022年にかけての米国の5027人のAmazon.comユーザーの購入履歴。データセットのサイズは300MB超。"
    analysis_goal = "['Shipping Adress State', 'y', 'Category']の順でドリルダウンすることによる分析" # , 'Category'
    focus_attr_l = ["Purchase Price Per Unit"] # "Quantity"
    ordinal_d = {"y":[2024, 2023, 2022, 2021, 2020, 2019, 2018]}
    s_node = ["_root"] # root以外の時は、ノードへのパス（例:["製造業","静岡県",2022]）となる。


    df_meta_info = DataFrameMetaInfo(
        df=df, 
        sample_df=sample_df, 
        df_description=df_description, 
        analysis_goal=analysis_goal,
        focus_attr_l=focus_attr_l
    )
    # print(df_meta_info.attr_type_d)
    # print(df_meta_info.operator_d)
    manager = DatafactManager()
    # TODO: make_tree, cal_subtree_nodesは後で一つの関数にまとめる
    # NOTE: tree_dはdrilldown_path_l[0]の属性が、ユニークな値を多く持つと計算時間がべらぼうにかかかる
    """
    例:("Category"属性,"Shipping Address State"属性)=(1872,53)←ユニークな値の数, dfの行数=200万弱
    drilldown_path = ["Category","Shipping Address State"]→make_tree計算時間60sec以上
    drilldown_path = ["Shipping Address State","Category"]→make_tree計算時間1sec以下
    """
    
    
    cal_subtree_nodes(s_node, manager, ordinal_d, df_meta_info)
    
    cal_subtree_significance(s_node, manager, ordinal_d, df_meta_info)
    for agg_attr in focus_attr_l:
        for f_num in df_meta_info.aggregation_f_d[agg_attr]:
            agg_f = F_NUM2F_NAME_D[f_num]
            datafact_l_to_verbalize = drilldown(s_node,manager,ordinal_d,df_meta_info,agg_attr,agg_f)
            section = Section((agg_attr,agg_f))
            section.make_section(datafact_l_to_verbalize,manager,ordinal_d,df_meta_info)

    # datafact_l_to_verbalize = drilldown(s_node, manager, ordinal_d, df_meta_info)


