import time
from datetime import datetime
import copy
# 自分で作った関数の読み込み
from others import filter_df_by_parents, filter_df_by_subject, is_datafacts
from logging_config import setup_logger
from make_operation import make_operations, make_operations_for_datafacts
from make_subject import make_subject
from datafact_model import Datafact
from debug import debug_datafact
from cal_significance import detect_outliers

logger = setup_logger()

"""
ドリルダウンする木を生成する関数
入力:
- ドリルダウンする属性のリスト
- data frame
- s_node: スタートするノードのパス（例:["_root"]←根を表す, ["静岡県",2022]）
出力:
- 木構造を保存した辞書。
    - キーはノードパスのタプル（例: (静岡県,2022)）
    - 値は子ノードのパス(例: [[静岡県,2022,製造業], [静岡県,2022,サービス業],..., [静岡県,2022,IT業]])
"""
# TODO: 要リファクタリング（読みづらいし、何がしたいのか分かりづらい）
def make_tree(drilldown_l, df, s_node=["_root"]):
    """
    attrsで親ノードのパスを受け取り、その子のパス群のリストを返す
    入力:
    - drilldown_l: ["県","年","大分類"]
    - attrs: 親ノードのパス（例 ["静岡県",2021"]）
    - df: 親ノードのdf(例 県==静岡 and 年==2021でfilterされたもの)
    出力
    - df_parent: parentの部分空間におけるdf
    - リスト: parentの子ノードへのパスを格納したリスト
        [
            ["静岡県",2021","製造業"],
            ["静岡県",2021","サービス業"],
            ...
            ["静岡県",2021","IT業"],
        ]
    """
    def return_children(drilldown_l=drilldown_l, attrs=None, df_parent=None):
        if(attrs==['_root']):
            children_l = list(df_parent[drilldown_l[0]].unique())
            return [[c] for c in children_l if(str(c)!="nan")]
        if(len(drilldown_l)==len(attrs)):
            return []
        children_l = list(df_parent[drilldown_l[len(attrs)]].unique())
        return [attrs + [c] for c in children_l if(str(c)!="nan")]

    print(f"\n{datetime.fromtimestamp(time.time())}::ドリルダウンする木構造の作成を開始")
    df_d = {tuple(s_node):df}
    tree_d = {tuple(s_node):return_children(attrs=s_node,df_parent=df)}
    all_nodes = copy.deepcopy(tree_d[tuple(s_node)])
    while all_nodes:
        c_node = all_nodes.pop()
        key = tuple(c_node[:-1]) if(len(c_node)>1) else tuple(['_root'])
        if(key not in df_d):
            subject = make_subject(c_node, drilldown_l)
            key2 = tuple(c_node[:-2])if(len(c_node)>2) else tuple(['_root'])
            df_d[key] = filter_df_by_subject(subject=subject, df=df_d[key2])  # df_d[key] = filter_df_by_subject(subject=subject, df=df)
        df_parent = df_d[key] 
        children_l = return_children(attrs=c_node, df_parent=df_parent)
        if(children_l != []):
            tree_d[tuple(c_node)] = children_l
            all_nodes += children_l
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
    """
    datafactsの列挙を実施
    """
    def list_datafacts(s_node, datafacts_l):
        for c_node in tree_d[tuple(s_node)]:
            """
            c_nodeが末端ノードではない時に、c_nodeの子達をdatafactsとしてまとめる
            - subject=[{},"C",["*"]] 
            - subject=[{"A":"a"},"C",["*"]] 
            """
            if(tuple(c_node) in tree_d):
                node_path = copy.deepcopy(c_node)
                node_path[-1] = "*"
                subject = make_subject(node_path=node_path, drilldown_attr=drilldown_path_l)
                operation_l = make_operations_for_datafacts(agg_attrs, agg_f_d, operator_d, subject, ordinal_d, attr_type)
                for operation in operation_l:
                    datafacts = Datafact(subject=subject, operation=operation)
                    datafacts_l.append(datafacts)
            """
            ルートから見て、c_nodeが孫以降のノードである時、以下をdatafactsとしてまとめる
            - subject=[{"A":"a","B":"*"},"C",["c"]] 
            - subject=[{"A":"*"},"B",["b"]] 
            ↓ これは考えない。
            - subject=[{"A":"*","B":"*"},"C",["c"]] 
            """
            if(len(c_node)>=2):
                node_path = copy.deepcopy(c_node)
                node_path[-2] = "*"
                subject = make_subject(node_path=node_path, drilldown_attr=drilldown_path_l)
                operation_l = make_operations_for_datafacts(agg_attrs, agg_f_d, operator_d, subject, ordinal_d, attr_type)
                for operation in operation_l:
                    datafacts = Datafact(subject=subject, operation=operation)
                    datafacts_l.append(datafacts)
            
            if(tuple(c_node) in tree_d):
                list_datafacts(c_node, datafacts_l)
            else:
                return
        return datafacts_l
    
    """
    datafactsに含まれるdatafactを集めてくる
    """
    def collect_values(datafacts):
        values = manager.search_result(datafacts.subject, datafacts.operation)
        if(values is not None):
            return values
        values = {}
        parents, col_name, filter_values = datafacts.subject
        operation_name, *operation_others = datafacts.operation
        flg, key = is_datafacts(datafacts.subject)
        # print(f'drilldown.py:\n{key}')

        if(operation_name=='Aggregation'):
            if(values==['*']):
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
            operation_ = datafact1.operation
            if(values==['*']): # c_node:['静岡県',2018], ['静岡県',2019], ..
                node_path = tuple([v for _, v in parents.items()])
            elif(flg): # c_node:['静岡県','製造業',2018], ['静岡県','サービス業',2018], ..
                node_path = tuple(["_root"]) if(len(parents)==1) else tuple([k for k in parents.keys() if (k!=key)][0])
            else:
                raise ValueError('datafactsではありません')

            for c_node in tree_d[node_path]: 
                c_subject = make_subject(c_node, drilldown_path_l)
                c_parents, c_col, c_filter_values = c_subject
                n = ordinal_d[c_col].index(c_filter_values[0])
                subject1, subject2 = c_subject, [c_parents,c_col,[ordinal_d[c_col][n+1]]]
                c_operation = [
                    "ScalarArithmetic", 
                    op, 
                    Datafact(subject1, operation_),
                    Datafact(subject2, operation_)
                ]
                values[c_node[-1]] = manager.search_result(c_subject, c_operation)
            return values
        else:
            raise ValueError(f'想定していないoperation_nameです:{operation_name}')

    datafacts_l = list_datafacts(s_node, [])
    for datafacts in datafacts_l:
        values = collect_values(datafacts)
        logger.info(f'drilldown.py:\n{debug_datafact(datafacts)}')
        logger.info(f'drilldown.py:\n{values}')
    return 
"""
ドリルダウンを実行する関数
入力:
- 始点ノード
- 木構造のd
- 属性のタイプ(Categoricalとか)情報のd
- df

"""