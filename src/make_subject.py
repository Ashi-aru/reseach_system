import pandas as pd
# 自分で作成した関数のインポート
from others import filter_df_by_parents

"""
parentsとドリルダウンの属性名を受け取り、datafactのsubjectを生成
入力:(parents, drilldown_attribute, df, flg)=({"県":"東京都", "年":2022}, "大分類", df)
出力:[
    [{"県":"東京都", "年":2022}, "大分類", "製造業"],
    [{"県":"東京都", "年":2022}, "大分類", "サービス業"],
    [{"県":"東京都", "年":2022}, "大分類", "漁業"],
    ...
    ]
補足情報:
datafact := subject, operation
subject := [parents(親ノードの情報), ノードの属性名, ノードの属性値]
"""
def make_subject(parents, drilldown_attribute, df, flg=True):
    if(flg):
        df = filter_df_by_parents(panrents=parents,df=df)
    return [[parents, drilldown_attribute, value] for value in df[drilldown_attribute].unique()]

