import time
from datetime import datetime
from collections import deque
import copy
# 自分で作った関数の読み込み
from others import filter_df_by_subject, is_datafacts, is_agg_attr_operation
from logging_config import setup_logger
from make_operation import make_operations, make_operations_for_datafacts
from make_subject import make_subject
from datafact_model import Datafact
from debug import debug_datafact
from cal_significance import detect_outliers

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








"""
ドリルダウンを実行する関数。被Agg属性ごとに1-p値に基づいたドリルダウンを実施。
通過したノードのうち、被Agg属性関連のdatafactをdatafact_lに格納する。

【入力】
- 始点ノード
- 木構造のd
- DataFactManagerインスタンス
- 属性のタイプ(Categoricalとか)情報のd
- ordinal_d
- df
【出力】
- datafact_l:言語化するデータファクトを格納したリスト
"""
def drilldown(s_node, manager, ordinal_d, df_meta_info):
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
        dd_nodes_num_l = [3,2,1] if(len(drilldown_path_l)==3) else [4,2] # 各階層でのドリルダウンノード数
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
            c_node_num = dd_nodes_num_l[len(node)] if(node!=['_root']) else dd_nodes_num_l[0]
            for i in range(c_node_num):
                if(len(children_p_value_d)==0):
                    break
                c_node, _ = children_p_value_d.popitem()
                print(f'drilldown.py::bfs\n{c_node},{_}')
                queue.append(c_node)
        return textdatafact_l

    print(f"\n{datetime.fromtimestamp(time.time())}::drilldown開始。")
    textdatafact_l = []
    for agg_attr in agg_attrs:
        for f_num in agg_f_d[agg_attr]:
            agg_f = F_NUM2F_NAME_D[f_num]
            textdatafact_l = bfs(s_node, agg_attr, agg_f, textdatafact_l)
    print(f'drilldown.py::drilldown\n言語化するデータファクト数={len(textdatafact_l)}')
    print(f"{datetime.fromtimestamp(time.time())}::drilldown終了。")
    return textdatafact_l




