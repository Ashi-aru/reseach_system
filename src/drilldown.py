import pandas as pd
# 自分で作った関数の読み込み
from others import filter_df_by_parents


"""
ドリルダウンする木を生成する関数
入力:
- ドリルダウンする属性のリスト
- data frame
出力:
- 木構造を保存した辞書
"""
def make_tree(drilldown_l, df):
    """
    attrsで親ノードのパスを受け取り、その子のパス群のリストを返す
    入力:
    - drilldown_l: ["県","年","大分類"]
    - attrs: ["静岡県",2021"]
    出力
    - [
        ["静岡県",2021","製造業"],
        ["静岡県",2021","サービス業"],
        ...
        ["静岡県",2021","IT業"],
    ]
    """
    def make_subtree(drilldown_l=drilldown_l, attrs=[], df=df):
        if(len(drilldown_l)==len(attrs)):
            return []
        parents = dict([(drilldown_l[i],v) for i, v in enumerate(attrs)])
        df_tmp = filter_df_by_parents(parents=parents, df=df)
        children_l = list(df_tmp[drilldown_l[len(attrs)]].unique())
        return [attrs + [c] for c in children_l if(str(c)!="nan")]
    
    tree_d = {"root":make_subtree()}
    all_nodes = tree_d["root"]
    while all_nodes:
        node = all_nodes.pop()
        children_l = make_subtree(attrs=node)
        if(children_l != []):
            tree_d[tuple(node)] = children_l
            all_nodes += children_l
    return tree_d
