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
from datafact_model import Datafact
from drilldown import cal_subtree_nodes, make_tree, cal_subtree_significance


PROJ_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJ_DIR/"data"

logger = setup_logger()

if(__name__ == '__main__'):
    # NOTE: ここはユーザーからの入力を受け取る形にする
    table = '_amazon-purchases.csv'
    df = pd.read_csv(DATA_DIR/f'tables/{table}')
    sample_df = df.head(2)
    df_description = "2018年から2022年にかけての米国の5027人のAmazon.comユーザーの購入履歴。データセットのサイズは300MB超。"
    analysis_goal = "['Shipping Adress State', 'y', 'Category']の順でドリルダウンすることによる分析"
    focus_attr_l = ["Purchase Price Per Unit","Quantity"]
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
    print(f"\n{datetime.fromtimestamp(time.time())}::木構造の計算を開始")
    tree_d = make_tree(drilldown_attr=df_meta_info.drilldown_path_l, df_p=df_meta_info.df)
    print(f'{datetime.fromtimestamp(time.time())}::len(tree_d)={len(tree_d)}')
    print(f"{datetime.fromtimestamp(time.time())}::木構造の計算を終了")
    s = time.time()
    print(f"\n{datetime.fromtimestamp(time.time())}::各ノードの計算を開始")
    cal_subtree_nodes(s_node, tree_d, manager, ordinal_d, df_meta_info, df_meta_info.df)
    e = time.time()
    print(f"{datetime.fromtimestamp(time.time())}::各ノードの計算を終了。\n計算時間 = {e-s}s")
    # cal_subtree_significance(s_node, tree_d, manager, ordinal_d, df_meta_info)
    # result1 = manager.search_result(
    #     subject = [{'y':2022},'Shipping Address State',['*']],
    #     operation = ["Aggregation", "Purchase Price Per Unit", "mean"]
    # )
    # result2 = manager.search_result(
    #     subject = [{'y':2022},'Shipping Address State',['MA']],
    #     operation = ["Aggregation", "Purchase Price Per Unit", "mean"]
    # )
    # result3 = manager.search_result(
    #     subject = [{'y':2022},'Shipping Address State',['MA']],
    #     operation = [
    #         'Rank', 
    #         '降順',
    #         Datafact(
    #             subject=[{'y':2022},'Shipping Address State',['*']],
    #             operation=["Aggregation", "Purchase Price Per Unit", "mean"]
    #         )
    #     ]
    # )
    # # logger.info(result1)
    # # logger.info(result2)
    # # logger.info(result3)

    # logger.info(f'run.py:\n{manager.results}')

