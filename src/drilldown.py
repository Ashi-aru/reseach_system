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
def cal_subtree_nodes(s_node, tree_d, manager, ordinal_d, df_meta_info, df):
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
        operations = make_operations(agg_attrs, agg_f_d, operator_d, subject, ordinal_d, step_n, attr_type)
        for operation in operations:
            datafact = Datafact(subject=subject, operation=operation)
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

    for step_n in range(1,4):
        print(f'step_{step_n}')
        logger.info(f'\n====================================step_{step_n}===============================\n')
        dfs(node=s_node,df=df,step_n=step_n)


"""
各ノードにおける重要度計算
入力:
- s_node: 始点ノードのパス (例: [製造業,静岡県,2022])
- tree_d: 木構造のd
- manager: DatafactManagerインスタンス
- ordinal_d:
- df_meta_info:DataFrameMetaInfoインスタンス
    - attr_type_d: 属性のタイプ(Categoricalとか)情報のd
    - agg_attrs: 計算を実施する属性を格納したリスト
    - agg_f_d: 被計算属性ごとにaggregation関数を保存した辞書
    - operator_d: (被計算属性,aggregation_f)ごとにScalarArithmetic用演算子を格納した辞書
    - drilldown_path_l: ドリルダウンパス
"""
def cal_subtree_significance(s_node, tree_d, manager, ordinal_d, df_meta_info):
    agg_attrs = df_meta_info.focus_attr_l
    agg_f_d = df_meta_info.aggregation_f_d
    operator_d = df_meta_info.operator_d
    attr_type = df_meta_info.attr_type_d
    drilldown_path_l = df_meta_info.drilldown_path_l
    df = df_meta_info.df    
    """
    datafactsの列挙を実施
    """
    def list_datafacts(s_node):
        """
        flg_d:[{A:a,B:*},C,[c]], [{B:*},C,[c]]を列挙するときに、ダブりをなくすためのflgを生成
        - キーは(a,c) or ('_root',c)
        - valueはTrue/False（まだ通過してなかったらTrue）
        """
        def make_flg_d():
            flg_d = {}
            if(len(drilldown_path_l)>=2):
                for v in df[drilldown_path_l[1]].unique():
                    flg_d[('_root',v)] = True
            if(len(drilldown_path_l)>=3):
                for v1 in df[drilldown_path_l[0]].unique():
                    for v2 in df[drilldown_path_l[2]].unique():
                        flg_d[(v1,v2)] = True
            return flg_d

        def dfs(p_node, datafacts_l=[]):
            """
            p_nodeが末端ノードではない時に、p_nodeの子達をdatafactsとしてまとめる
            - subject=[{},"C",["*"]] 
            - subject=[{"A":"a"},"C",["*"]] 
            """
            if(tuple(p_node) in tree_d):
                node_path = copy.deepcopy(p_node)
                if(node_path==["_root"]):
                    node_path = []
                node_path.append('*')
                subject = make_subject(node_path=node_path, drilldown_attr=drilldown_path_l)
                operation_l = make_operations_for_datafacts(agg_attrs, agg_f_d, operator_d, subject, ordinal_d, attr_type)
                for operation in operation_l:
                    datafacts = Datafact(subject=subject, operation=operation)
                    datafacts_l.append(datafacts)
                # print(datafacts_l)
            """
            ルートから見て、p_nodeが孫以降のノードである時、以下をdatafactsとしてまとめる
            - subject=[{"A":"a","B":"*"},"C",["c"]] 
            - subject=[{"A":"*"},"B",["b"]] 
            ↓ これは考えない。
            - subject=[{"A":"*","B":"*"},"C",["c"]] 
            """
            if(len(p_node)>=2):
                flg_key = (p_node[0],p_node[-1]) if(len(p_node)>2) else ('_root', p_node[-1])
                if(flg_d[flg_key]):
                    flg_d[flg_key] = False
                    node_path = copy.deepcopy(p_node)
                    node_path[-2] = "*"
                    subject = make_subject(node_path=node_path, drilldown_attr=drilldown_path_l)
                    operation_l = make_operations_for_datafacts(agg_attrs, agg_f_d, operator_d, subject, ordinal_d, attr_type)
                    for operation in operation_l:
                        datafacts = Datafact(subject=subject, operation=operation)
                        datafacts_l.append(datafacts)
            """
            再帰的に上の処理を繰り返す
            """
            if(tuple(p_node) not in tree_d):
                return datafacts_l
            for c_node in tree_d[tuple(p_node)]:
                datafacts_l = dfs(c_node, datafacts_l)
            return datafacts_l
        
        flg_d = make_flg_d()
        datafacts_l = dfs(p_node=s_node)
        return datafacts_l


    """
    datafactsに含まれるdatafactを集めてくる
    """
    def collect_values(datafacts):
        values = manager.search_result(datafacts.subject, datafacts.operation)
        if(values is not None):
            # filter_values=['*']の時はほとんどここで蹴りがつく。
            return values
        values = {}
        parents, col_name, filter_values = datafacts.subject
        operation_name, *operation_others = datafacts.operation
        flg, key = is_datafacts(datafacts.subject)
        # print(f'drilldown.py:\n{key}')

        if(operation_name=='Aggregation'):
            if(filter_values==['*']):
                node_path = tuple([v for _, v in parents.items()])
            elif(flg and len(parents)==1):
                node_path = tuple(["_root"])
            elif(flg and len(parents)==2):
                node_path = tuple([v for k,v in parents.items() if (k!=key)])
            else: 
                raise ValueError('datafactsではありません')

            for c_node in tree_d[node_path]:
                    c_subject = make_subject(c_node, drilldown_path_l)
                    values[c_node[-1]] = manager.search_result(c_subject, datafacts.operation)
            return values

        elif(operation_name=='ScalarArithmetic'):
            op, datafact1, datafact2 = operation_others
            if(filter_values==['*']): # c_node:['静岡県',2018], ['静岡県',2019], ..
                node_path = tuple([v for _, v in parents.items()])
                for c_node in tree_d[node_path]: 
                    c_subject = make_subject(c_node, drilldown_path_l)
                    c_parents, c_col, c_filter_values = c_subject
                    n = ordinal_d[c_col].index(c_filter_values[0])
                    subject1, subject2 = c_subject, [c_parents,c_col,[ordinal_d[c_col][n+1]]]
                    c_operation = [
                        "ScalarArithmetic", 
                        op, 
                        Datafact(subject1, datafact1.operation),
                        Datafact(subject2, datafact2.operation)
                    ]
                    values[c_node[-1]] = manager.search_result(c_subject, c_operation)
            elif(flg): # c_node:['静岡県','製造業',2018], ['静岡県','サービス業',2018], ..
                node_path = tuple(["_root"]) if(len(parents)==1) else tuple([k for k in parents.keys() if (k!=key)][0])
                for c_node in tree_d[node_path]:
                    c_node = copy.deepcopy(c_node)
                    c_node.append(filter_values[0])
                    c_subject = make_subject(c_node, drilldown_path_l)
                    c_parents, c_col, c_filter_values = c_subject
                    n = ordinal_d[c_col].index(c_filter_values[0])
                    subject1, subject2 = c_subject, [c_parents,c_col,[ordinal_d[c_col][n+1]]]
                    c_operation = [
                        "ScalarArithmetic", 
                        op, 
                        Datafact(subject1, datafact1.operation),
                        Datafact(subject2, datafact2.operation)
                    ]
                    values[c_node[-1]] = manager.search_result(c_subject, c_operation)
            else:
                raise ValueError('datafactsではありません')

            return values
        else:
            raise ValueError(f'想定していないoperation_nameです:{operation_name}')

    """
    datafactsの*をvで置き換え、datafactを返す関数
    """
    def replace_star_with_key(datafacts, v):
        parents, col_name, filter_values = datafacts.subject
        operation_name, *operation_others = datafacts.operation
        flg, key_attr = is_datafacts(datafacts.subject)
        if(operation_name=="Aggregation"):
            if(filter_values==['*']):
                datafact = Datafact(
                    subject=[parents, col_name, [v]],
                    operation=datafacts.operation
                )
            elif(flg):
                parents = copy.deepcopy(parents)
                parents[key_attr]=v
                datafact = Datafact(
                    subject=[parents, col_name, filter_values],
                    operation=datafacts.operation
                )
            else:
                raise ValueError('datafactsではありません')
        elif(operation_name=='ScalarArithmetic'):
            if(filter_values==['*']):
                op, datafact1, datafact2 = operation_others
                n = ordinal_d[col_name].index(v)
                subject1, subject2 = [parents, col_name, [v]], [parents, col_name, [ordinal_d[col_name][n+1]]]
                datafact = Datafact(
                    subject=[parents, col_name, [v]],
                    operation=[
                        op,
                        Datafact(subject1, datafact1.operation),
                        Datafact(subject2, datafact2.operation)
                    ]
                )
            elif(flg):
                parents = copy.deepcopy(parents)
                parents[key_attr] = v
                op, datafact1, datafact2 = operation_others
                _, col_name1, filter_values1 = datafact1.subject
                _, col_name2, filter_values2 = datafact2.subject
                subject1, subject2 = [parents, col_name1, filter_values1], [parents, col_name2, filter_values2]
                datafact = Datafact(
                    subject=[parents, col_name, filter_values],
                    operation=[
                        op,
                        Datafact(subject1, datafact1.operation),
                        Datafact(subject2, datafact2.operation)
                    ]
                )
            else:
                raise ValueError('datafactsではありません')
        else:
            raise ValueError(f'想定していないoperation_nameです:{operation_name}')
        return datafact
    
    print(f"\n{datetime.fromtimestamp(time.time())}::datafactsの列挙開始。")
    datafacts_l = list_datafacts(s_node)
    print(f'len(datafacts_l)={len(datafacts_l)}')
    print(f"{datetime.fromtimestamp(time.time())}::datafactsの列挙終了。")
    print(f"\n{datetime.fromtimestamp(time.time())}::重要性の計算を開始。")
    s = time.time()
    for datafacts in datafacts_l:
        values = collect_values(datafacts) # とはいえこっちも10秒ほどかかる
        outliers = detect_outliers(values) # こっちに時間がかかる。外れ値検定の要素数が増えると、外れ値の計算を何周もやることになる?
        # logger.info(f'drilldown.py:\n{debug_datafact(datafacts)}')
        # logger.info(f'drilldown.py:\n{values}')
        # logger.info(f'drilldown.py:\n{outliers}')
        for k, v_d in outliers.items():
            datafact = replace_star_with_key(datafacts, k)
            p = v_d['p_value']
            manager.update_significances(datafact.subject, datafact.operation, 1-p)
    e = time.time()
    print(f"\n{datetime.fromtimestamp(time.time())}::重要性の計算を終了。\n計算時間={e-s}")
    # logger.info(f'drilldown.py:\n{manager.significances}')
    return 

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
def drilldown(s_node, tree_d, manager, ordinal_d, df_meta_info):
    agg_attrs = df_meta_info.focus_attr_l
    agg_f_d = df_meta_info.aggregation_f_d
    operator_d = df_meta_info.operator_d
    attr_type = df_meta_info.attr_type_d
    drilldown_path_l = df_meta_info.drilldown_path_l
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




