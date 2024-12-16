import pandas as pd
from pathlib import Path
from datetime import date
# 自分で作成した関数のインポート
from others import filter_df_by_parents
from logging_config import setup_logger


PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/"data"
TODAY = date.today().strftime("%Y-%m-%d")

looger = setup_logger()

"""
ノードへのパスを受け取り、ノードのsubjectを出力する関数
入力:
- ノードへのパス: ["製造業","静岡県",2022]、["製造業"]、["製造業","*","2022"]
- drilldown_attr: ["大分類","都道府県","年"]
出力:
- subject: [{"大分類":"製造業", "都道府県":"静岡県"}, "年", ["2022"]]、[{}, "大分類", ["製造業"]]、[{"大分類":"製造業", "都道府県":"*"}, "年", ["2022"]]
"""
def make_subject(node_path, drilldown_attr):
    if(node_path=="root"):
        return [{},"",[]]
    parents,filter_value = node_path[:-1], node_path[-1]
    parents = dict([(drilldown_attr[i], x) for i, x in enumerate(parents)])
    return [parents, drilldown_attr[len(node_path)-1], [filter_value]]