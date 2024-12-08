import pytest
import sys
import pandas as pd
import numpy as np
from pathlib import Path
import random
sys.path.append('/Users/ashikawaharuki/Desktop/research/TDB/test/system/src')
# 自分で作成したクラス、関数をimport
from datafact_model import Datafact
from datafact_manager import DatafactManager
from make_templates import make_templates
import datafact_data


PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/'data'

ordinal_d = {"y":['2024', '2023', '2022', '2021', '2020', '2019', '2018']}

manager = DatafactManager()
datafact_l = [
    datafact_data.convert_datafact_1,
    datafact_data.convert_datafact_2,
    datafact_data.convert_datafact_3,
    datafact_data.convert_datafact_4,
    datafact_data.convert_datafact_5,
]

table_description = """
このデータセットは、2018年から2022年にかけて5027人のアメリカ在住Amazonユーザーの購入履歴（amazon-purchases.csv）を含んでいます。ユーザーごとの人口統計情報や消費者レベルの変数（オンライン調査から収集、survey.csvに記録）も含まれています。Survey ResponseID列を通じて、購入履歴と調査データがリンクされています。

購入履歴には注文日、配送先州、購入価格、数量、商品コード（ASIN/ISBN）、商品名、カテゴリが含まれます。このデータは、ユーザーがAmazon.comから自身でエクスポートし、同意のもと研究者に提供されました。個人情報（PII）は共有前にユーザーのデバイス上で削除されています。
"""

class TestMakeTemplates:
    def setup_method(self):
        # 各メソッドを実行する際に呼び出される初期化関数
        None
    @pytest.mark.parametrize("datafact_l, manager, ordinal_d, table_description", [
        (datafact_l, manager, ordinal_d, table_description)
    ],ids = [f'case{i}' for i in range(1,2)]
    )
    def test_make_templates(self, datafact_l, manager, ordinal_d, table_description):
        make_templates(datafact_l, manager, ordinal_d, table_description)