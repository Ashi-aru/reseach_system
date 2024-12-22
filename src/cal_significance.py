from pathlib import Path
import math
import time
from scipy.stats import t
import scipy.stats as stats
import numpy as np
import random
from datetime import datetime, date
import copy
# 自分で作った関数の読み込み
from others import is_datafacts
from logging_config import setup_logger
from make_operation import make_operations_for_datafacts
from make_subject import make_subject
from datafact_model import Datafact
from debug import debug_datafact


PROJ_DIR = Path(__file__).resolve().parent.parent
TODAY = date.today().strftime("%Y-%m-%d")
logger = setup_logger()


"""
Grubbs検定で外れ値を検出し、そのp値を計算する関数。
以下の3ステップを繰り返す（[外れ値が検出されなくなる or valuesの長さが3未満になる]まで）
1. 平均から最も外れた値を選出
2. 外れ値の判定を行う
3. 外れ値の場合、p値を保存→この値をデータ群から除外

入力
- data: 数値のリスト
    - {"製造業":100, "サービス業":500,...,"その他":150}
- alpha:有意水準
出力
- 外れ値の辞書
    - {"サービス業":{"p_value":0.002, "data_value":150},..}
"""
def detect_outliers(data, alpha=0.05):
    """
    outliersライブラリを用いて複数の外れ値を最大値方向と最小値方向に対して検出し、
    p値を計算して出力する。
    """
    data = dict([(k,v) for k,v in data.items() if v is not None])
    result_d = {}
    while len(data) >= 3:  # Grubbs検定には少なくとも3つのデータが必要
        values, keys = list(data.values()), list(data.keys())
        n = len(values)
        mean, std = np.mean(values), np.std(values)

        abs_diff = np.abs(values - mean)
        max_diff_index = np.argmax(abs_diff)
        suspect_value, suspect_name = values[max_diff_index], keys[max_diff_index]

        G = abs(suspect_value - mean) / std
        if((n-1)**2 - n*(G**2)>0):
            t_value = math.sqrt(n*(n-2)*(G**2) / ((n-1)**2 - n*(G**2)))
            p_value = (1 - t.cdf(t_value, n-2))*(2*n)
        else:
            """
            t_valueのルートの中身が負になる場合はp値を0にする。
            G > (n-1)/sqrt(n)の時に、ルートの中身が0になる。
            G = abs(x-mean)/stdより、abs(x-mean)が大きすぎるとルートの中身が0になる
            → めちゃくちゃ外れ値ということなので、p値は0でいいのではないでしょうか？
            """
            p_value = 0  # 計算できない場合は0を代入
        
        if(p_value <= alpha):
            del data[suspect_name]
            result_d[suspect_name] = {"p_value": p_value, "data_value": suspect_value}
        else:
            break  
    return result_d



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
def cal_subtree_significance(s_node, manager, ordinal_d, df_meta_info):
    agg_attrs = df_meta_info.focus_attr_l
    agg_f_d = df_meta_info.aggregation_f_d
    operator_d = df_meta_info.operator_d
    attr_type = df_meta_info.attr_type_d
    drilldown_path_l = df_meta_info.drilldown_path_l
    df = df_meta_info.df    
    tree_d = df_meta_info.tree_d
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


if(__name__=="__main__"):
    def smirnov_grubbs(data, alpha):
        x, o = list(data), []
        while True:
            n = len(x)
            t = stats.t.isf(q=(alpha / n) / 2, df=n - 2)
            tau = (n - 1) * t / np.sqrt(n * (n - 2) + n * t * t)
            i_min, i_max = np.argmin(x), np.argmax(x)
            myu, std = np.mean(x), np.std(x, ddof=1)
            i_far = i_max if np.abs(x[i_max] - myu) > np.abs(x[i_min] - myu) else i_min
            tau_far = np.abs((x[i_far] - myu) / std)
            if tau_far < tau: break
            o.append(x.pop(i_far))
        return o #(np.array(x), np.array(o))


    # データの基本設定
    normal_data = [random.uniform(0, 500) for _ in range(1000)]  # 正常値
    outliers = [random.uniform(800, 1000), random.uniform(-10, 0)]  # 外れ値
    # 正常値と外れ値を混ぜる
    data = normal_data + outliers
    random.shuffle(data) 
    # 実行
    alpha = 0.05
    print(smirnov_grubbs(data, alpha))
    print(detect_outliers(data, alpha))