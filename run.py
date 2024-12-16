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
from drilldown import cal_subtree_nodes, make_tree


PROJ_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJ_DIR/"data"

logger = setup_logger()

if(__name__ == '__main__'):
    # NOTE: ここはユーザーからの入力を受け取る形にする
    table = '_amazon-purchases.csv'
    df = pd.read_csv(DATA_DIR/f'tables/{table}')
    sample_df = df.head(2)
    df_description = "2018年から2022年にかけての米国の5027人のAmazon.comユーザーの購入履歴。データセットのサイズは300MB超。"
    analysis_goal = "地理的な購買パターンの分析"
    focus_attr_l = ["Purchase Price Per Unit","Quantity"]
    ordinal_d = {"y":['2024', '2023', '2022', '2021', '2020', '2019', '2018']}
    s_node = "root" # root以外の時は、ノードへのパス（例:["製造業","静岡県",2022]）となる。


    df_meta_info = DataFrameMetaInfo(
        df=df, 
        sample_df=sample_df, 
        df_description=df_description, 
        analysis_goal=analysis_goal,
        focus_attr_l=focus_attr_l
    )
    manager = DatafactManager()
    # TODO: make_tree, cal_subtree_nodesは後で一つの関数にまとめる
    tree_d = make_tree(drilldown_l=df_meta_info.drilldown_path_l, df=df_meta_info.df)
    print(f"\n{datetime.fromtimestamp(time.time())}::各ノードの計算を開始")
    cal_subtree_nodes(s_node, tree_d, manager, ordinal_d, df_meta_info, df_meta_info.df)
    logger.info(manager.results)

