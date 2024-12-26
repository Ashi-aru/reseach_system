import time
from datetime import datetime
from collections import deque
# 自分で作った関数の読み込み
from others import is_aggattr_and_aggf_operation
from logging_config import setup_logger
from make_operation import make_operations
from make_subject import make_subject
from datafact_model import Datafact
from debug import debug_datafact, debug_operation

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
- plus_values: 1-p値に関する重みを保存した辞書。追加で加算する重みを保存
    - キーはノードのパスのタプル、値は重み
【出力】
- datafact_l:言語化するデータファクトを格納したリスト
"""
def drilldown(s_node, manager, ordinal_d, df_meta_info, agg_attr, agg_f, plus_values=None):
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
            etc = plus_values[tuple(c_node)] if(tuple(c_node) in plus_values) else 0 # c_nodeがfocus_nodesに含まれる時、重みを加算
            if(c_subject_key in manager.significances):
                c_p_value_d[tuple(c_node)] = sum(manager.significances[c_subject_key].values()) + etc # (被Agg属性,Aggregation_f)ごとのp値に基づいてドリルダウンするなら、ここも取捨選択が必要
            elif(etc>0):
                c_p_value_d[tuple(c_node)] = etc
            else:
                continue
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
        dd_nodes_num_l = [3,2,1] if(len(drilldown_path_l)==3) else [4,4] # 各階層でのドリルダウンノード数
        queue = deque()
        queue.append(s_node)
        while queue:
            node = queue.popleft()
            """
            まずはこのノードに関する言語化の準備
            """
            subject = make_subject(node_path=node, drilldown_attr=drilldown_path_l)
            for i in range(1,4):
                for operation in make_operations(agg_attrs, agg_f_d, operator_d, subject, ordinal_d, i, attr_type, manager):
                    if(not is_aggattr_and_aggf_operation(operation, agg_attr, agg_f)): # operationがagg_attr関連かつagg_f関連でない時
                        continue
                    datafact = Datafact(subject=subject, operation=operation, manager=manager)
                    textdatafact_l.append(datafact)
            """
            子ノードの探索
            """
            if(tuple(node) not in tree_d): # nodeが末端ノードの場合
                continue
            children_p_value_d = sort_children_by_total_p_value(node)
            c_node_num = dd_nodes_num_l[len(node)] if(node!=['_root']) else dd_nodes_num_l[0]
            for _ in range(c_node_num):
                if(len(children_p_value_d)==0):
                    break
                c_node, total_p = children_p_value_d.popitem()
                # print(f'drilldown.py::bfs\n{c_node},{total_p}')
                queue.append(c_node)
        return textdatafact_l

    print(f"\n{datetime.fromtimestamp(time.time())}::drilldown開始。")
    textdatafact_l = []
    # for agg_attr in agg_attrs:
    #     for f_num in agg_f_d[agg_attr]:
    #         agg_f = F_NUM2F_NAME_D[f_num]
    #         textdatafact_l = bfs(s_node, agg_attr, agg_f, textdatafact_l)
    textdatafact_l = bfs(s_node, agg_attr, agg_f, textdatafact_l)
    print(f'drilldown.py::drilldown\n言語化するデータファクト数={len(textdatafact_l)}')
    print(f"{datetime.fromtimestamp(time.time())}::drilldown終了。")
    return textdatafact_l




