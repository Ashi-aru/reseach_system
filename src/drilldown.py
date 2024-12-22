import pandas as pd
from pathlib import Path
from datetime import date, datetime
from collections import deque
import time
# 自分で定義した関数・クラスをimport
from logging_config import setup_logger
from debug import debug_datafact
from datafact_model import Datafact
from make_subject import make_subject
from make_operation import make_operations
from others import is_agg_attr_operation

PROJ_DIR = Path(__file__).resolve().parent.parent
TODAY = date.today().strftime("%Y-%m-%d")

logger = setup_logger()


"""
ドリルダウンを実行する関数。(被Agg属性,Aggregation_f)の組み合わせごとに1-p値に基づいたドリルダウンを実施。
通過したノードのうち、(被Agg属性,Aggregation_f)に関連するもののみを言語化するデータファクトとして返す。

【入力】
- 始点ノード
- 木構造のd
- DataFactManagerインスタンス
- 属性のタイプ(Categoricalとか)情報のd
- ordinal_d
- df
- agg_attr: 被Agg属性
- agg_f: 被Agg属性をAggregationする関数
【出力】
- datafact_l:言語化するデータファクトを格納したリスト
"""
def drilldown(s_node, manager, ordinal_d, df_meta_info, agg_attr, agg_f):
    agg_attrs = df_meta_info.focus_attr_l
    agg_f_d = df_meta_info.aggregation_f_d
    operator_d = df_meta_info.operator_d
    attr_type = df_meta_info.attr_type_d
    drilldown_path_l = df_meta_info.drilldown_path_l
    tree_d = df_meta_info.tree_d
    """
    p_nodeの子ノード(c_node)たちのp_value合計値を求め、c_nodeをキー、1-p_value合計値を値とした昇順ソート済みdを返す関数。
    後でd.popitems()で取り出すことで、1-p_valueが大きい順に取り出すことができる。
    【入力】
    - p_node: 親ノードのパス（例:[静岡県,2022], [静岡県]）
    【出力】
    - c_nodeをキー、1-p_value合計値を値とした昇順ソート済み辞書
    """
    def sort_children_by_total_p_value(p_node):
        children = tree_d[tuple(p_node)]
        c_p_value_d = {}
        for c_node in children:
            c_subject = make_subject(c_node, drilldown_path_l)
            c_subject_key = manager.make_key(c_subject, None)
            if(c_subject_key not in manager.significances):
                continue
            c_p_value_d[tuple(c_node)] = sum(manager.significances[c_subject_key].values())
        c_p_value_d = dict(sorted(c_p_value_d.items(), key=lambda x:x[1]))
        # NOTE:p_node==['_root']かつlen(c_p_value_d)<4の時、適当にドリルダウンするノードを追加する処理を入れる
        # if(p_node==['_root'] and len(c_p_value_d)==0):
        return c_p_value_d
    

    """
    s_nodeから始まる部分木に対して、agg_attr関連の1-p値で幅優先探索を実行する関数
    【入力】
    - agg_attr
    - text_datafact_l
    【出力】
    - text_datafact_l:言語化するデータファクトを
    """
    def bfs(s_node, agg_attr, agg_f, textdatafact_l=[]):
        drill_down_nodes_d = {}
        dd_nodes_num_l = [4,2,1] if(len(drilldown_path_l)==3) else [4,3] # 各階層でのドリルダウンノード数
        queue = deque()
        queue.append(s_node)
        while queue:
            node = queue.popleft()
            """
            まずはこのノードに関する言語化の準備
            """
            subject = make_subject(node_path=node, drilldown_attr=drilldown_path_l)
            for i in range(1,4):
                for operation in make_operations(agg_attrs, agg_f_d, operator_d, subject, ordinal_d, i, attr_type):
                    if(not is_agg_attr_operation(operation, agg_attr, agg_f)): # operationがagg_attr関連でない時
                        continue
                    datafact = Datafact(subject=subject, operation=operation)
                    textdatafact_l.append(datafact)
            """
            子ノードの探索
            """
            if(tuple(node) not in tree_d): # nodeが末端ノードの場合
                continue
            children_p_value_d = sort_children_by_total_p_value(node)
            drill_down_nodes_d[tuple(node)] = [c_node for c_node in children_p_value_d.keys()]
            c_node_num = dd_nodes_num_l[len(node)] if(node!=['_root']) else dd_nodes_num_l[0]
            for i in range(c_node_num):
                if(len(children_p_value_d)==0):
                    break
                c_node, _ = children_p_value_d.popitem()
                queue.append(c_node)
        return textdatafact_l, drill_down_nodes_d

    print(f"\n{datetime.fromtimestamp(time.time())}::drilldown開始。")
    textdatafact_l = []
    textdatafact_l = bfs(s_node, agg_attr, agg_f, textdatafact_l)
    print(f'drilldown.py::drilldown\n言語化するデータファクト数={len(textdatafact_l)}')
    print(f"{datetime.fromtimestamp(time.time())}::drilldown終了。")
    return textdatafact_l