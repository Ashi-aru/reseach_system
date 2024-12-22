from pathlib import Path
from datetime import date
import copy
import time
from datetime import datetime
# 自分が定義したクラス、関数をインポート
from logging_config import setup_logger


PROJ_DIR = Path(__file__).resolve().parent.parent
TODAY = date.today().strftime("%Y-%m-%d")
logger = setup_logger()


"""
ドリルダウンする木を生成する関数
入力:
- p_node: スタートするノードのパス（例:["_root"]←根を表す, ["静岡県",2022]）
- df_p: p_nodeの階層に沿って抽出されたdf
- drilldown_attr: ドリルダウンする属性のリスト(例:["県","年","大分類"])
- tree_d: 木構造を保存した辞書
出力:
- 木構造を保存した辞書。
    - キーはノードパスのタプル（例: (静岡県,2022)）
    - 値は子ノードのパス(例: [[静岡県,2022,製造業], [静岡県,2022,サービス業],..., [静岡県,2022,IT業]])
"""
def make_tree(p_node=['_root'], df_p=None, drilldown_attr=None, tree_d={}):
    c_nodes_l = []
    c_attr_name = drilldown_attr[len(p_node)] if(p_node!=["_root"]) else drilldown_attr[0]
    children = df_p[c_attr_name].unique()
    for child in children:
        c_node = copy.deepcopy(p_node) + [child] if(p_node!=["_root"]) else [child]
        c_nodes_l.append(c_node)
        if(len(c_node)<len(drilldown_attr)): # 末端ノードではないとき
            df_c = df_p[df_p[c_attr_name]==child]
            if(df_c.empty):
                continue
            make_tree(c_node, df_c, drilldown_attr,tree_d)
        else:
            continue
    tree_d[tuple(p_node)] = c_nodes_l
    return tree_d