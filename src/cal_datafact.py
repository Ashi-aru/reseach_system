import time
from datetime import datetime
# 自分で作った関数の読み込み
from others import filter_df_by_subject
from logging_config import setup_logger
from make_operation import make_operations
from make_subject import make_subject
from datafact_model import Datafact
from debug import debug_datafact

logger = setup_logger()

"""
始点ノードを根とする部分木について、各ノードでdatafact値の計算を実行する関数
入力
- s_node: 始点ノードのパス (例: [製造業,静岡県,2022])
- tree_d: 木構造のd
- manager: DatafactManagerインスタンス
- ordinal_d:
- df_meta_info:DataFrameMetaInfoインスタンス
    - df: データフレーム
    - attr_type_d: 属性のタイプ(Categoricalとか)情報のd
    - agg_attrs: 計算を実施する属性を格納したリスト
    - agg_f_d: 被計算属性ごとにaggregation関数を保存した辞書
    - operator_d: (被計算属性,aggregation_f)ごとにScalarArithmetic用演算子を格納した辞書
    - drilldown_path_l: ドリルダウンパス
"""
def cal_subtree_nodes(s_node, manager, ordinal_d, df_meta_info):
    tree_d = df_meta_info.tree_d
    df = df_meta_info.df
    """
    各ノードに関してoperationの列挙→datafact生成→計算実行→保存を行う関数
    入力
    - node: 関数を実施するノードへのパス
    - ordinal_d: 
    - df_meta_info: DataFrameMetaInfoインスタンス
    - manager: DatafactManagerインスタンス
    - df: データフレーム
    出力
    - None（各計算結果はhandle_datafactを実行時に、manager.resultsに保存）
    """
    def run_node_task(subject, df=None, step_n=1):
        agg_attrs = df_meta_info.focus_attr_l
        agg_f_d = df_meta_info.aggregation_f_d
        operator_d = df_meta_info.operator_d
        attr_type = df_meta_info.attr_type_d
        operations = make_operations(agg_attrs, agg_f_d, operator_d, subject, ordinal_d, step_n, attr_type, manager)
        for operation in operations:
            datafact = Datafact(subject=subject, operation=operation, manager=manager)
            # logger.info(f'drilldown.py:\n{debug_datafact(datafact)}')
            # print(debug_datafact(datafact))
            datafact.handle_datafact(manager, df, ordinal_d)
            # logger.info(f'drilldown.py:\n{manager.results}')
        return None
    
    def dfs(node, df, step_n):
        subject = make_subject(node_path=node, drilldown_attr=df_meta_info.drilldown_path_l)
        run_node_task(subject, df=df, step_n=step_n)
        if(tuple(node) not in tree_d):
            return
        df_parent = filter_df_by_subject(subject,df)
        for c_node in tree_d[tuple(node)]:
            dfs(c_node, df_parent, step_n)

    s = time.time()
    print(f"\n{datetime.fromtimestamp(time.time())}::各ノードの計算を開始")
    for step_n in range(1,4):
        print(f'step_{step_n}')
        logger.info(f'\n====================================step_{step_n}===============================\n')
        dfs(node=s_node,df=df,step_n=step_n)
    e = time.time()
    datafact_n = 0
    for k,v in manager.results.items():
        datafact_n += len(v)
    print(f"全datafact数: {datafact_n}")
    print(f"{datetime.fromtimestamp(time.time())}::各ノードの計算を終了。\n計算時間 = {e-s}s")
