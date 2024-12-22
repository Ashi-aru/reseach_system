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
from cal_datafact import cal_subtree_nodes
from cal_significance import cal_subtree_significance
from section import Section
from drilldown import drilldown

PROJ_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJ_DIR/"data"

logger = setup_logger()

if(__name__ == '__main__'):
    # NOTE: ここはユーザーからの入力を受け取る形にする
    table = '_amazon-purchases.csv'
    df = pd.read_csv(DATA_DIR/f'tables/{table}')
    sample_df = df.head(2)
    df_description = "2018年から2022年にかけての米国の5027人のAmazon.comユーザーの購入履歴。データセットのサイズは300MB超。"
    analysis_goal = "['y', 'Shipping Adress State', 'Category']の順でドリルダウンすることによる分析" # , 'Category'
    focus_attr_l = ["Purchase Price Per Unit"] # "Quantity",
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
    s = time.time()
    print(f"\n{datetime.fromtimestamp(time.time())}::各ノードの計算を開始")
    cal_subtree_nodes(s_node, manager, ordinal_d, df_meta_info, df_meta_info.df)
    e = time.time()
    print(f"{datetime.fromtimestamp(time.time())}::各ノードの計算を終了。\n計算時間 = {e-s}s")
    cal_subtree_significance(s_node, manager, ordinal_d, df_meta_info)
    for agg_attr in df_meta_info.focus_attr_l:
        for agg_f in df_meta_info.aggregation_f_d[agg_attr]:
            datafact_l, _ = drilldown(s_node, manager, ordinal_d, df_meta_info, agg_attr, agg_f)
            section = Section(attr_and_f_tuple=(agg_attr, agg_f))
            section.make_section(datafact_l, manager, ordinal_d, df_meta_info)



