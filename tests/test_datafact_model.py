import pytest
import sys
import pandas as pd
from pathlib import Path
import random


# 自分で作成したクラス、関数をimport
from datafact_model import Datafact
from datafact_manager import DatafactManager

sys.path.append('/Users/ashikawaharuki/Desktop/research/TDB/test/system/src')
PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/'data'

df = pd.read_csv(DATA_DIR/"tables/amazon-purchases.csv")
s = df[df["Shipping Address State"]=="MA"]["Category"].unique()
num_l = [i for i in range(1, len(s)+1)]
random.shuffle(num_l)
d = dict([(s[i-1], num_l[i-1]) for i in range(1, len(s)+1)])

DATAFACT1_1 = Datafact(
                subject=[{"Shipping Address State":"MA"}, "Category", ["ABIS_BOOK"]],
                operation=["Aggregation", "Purchase Price Per Unit", "mean"]
            )
DATAFACT1_2 = Datafact(
                subject=[{"Shipping Address State":"MA"}, "Category", ["ABIS_BOOK"]],
                operation=["Aggregation", "Purchase Price Per Unit", "sum_percent"]
            )
datafact_2_1_1 = Datafact(
                        subject=[{"Shipping Address State":"MA"}, "Category", ["ABIS_BOOK"]],
                        operation=["Aggregation", "Purchase Price Per Unit", "count_percent"]
                    )
datafact_2_1_2 = Datafact(
                        subject=[{"Shipping Address State":"MA"}, "Category", ["TOOTH_CLEANING_AGENT"]],
                        operation=["Aggregation", "Purchase Price Per Unit", "count_percent"]
                    )
DATAFACT2_1 = Datafact(
                subject=[{"Shipping Address State":"MA"}, "Category", ["ABIS_BOOK"]],
                operation=[
                    "ScalarArithmetic", 
                    "-",
                    datafact_2_1_1,
                    datafact_2_1_2,
                ]
            )
datafact_2_2_1 = Datafact(
                        subject=[{"Shipping Address State":"MA"}, "Category", ["ABIS_BOOK"]],
                        operation=["Aggregation", "Purchase Price Per Unit", "max"]
                    )
datafact_2_2_2 = Datafact(
                        subject=[{"Shipping Address State":"MA"}, "Category", ["TOOTH_CLEANING_AGENT"]],
                        operation=["Aggregation", "Purchase Price Per Unit", "max"]
                    )
DATAFACT2_2 = Datafact(
                subject=[{"Shipping Address State":"MA"}, "Category", ["ABIS_BOOK"]],
                operation=[
                    "ScalarArithmetic", 
                    "-",
                    datafact_2_2_1,
                    datafact_2_2_2,
                ]
            )
datafact_3_1_ = Datafact(
                        subject=[{"Shipping Address State":"MA"}, "Category", ["*"]],
                        operation = ["Aggregation", "Purchase Price Per Unit", "mean"]
                    )
DATAFACT3 = Datafact(
                subject=[{"Shipping Address State":"MA"}, "Category", ["ABIS_BOOK"]],
                operation=[
                    "Rank",
                    "降順",
                    datafact_3_1_
                ]
            )

class TestDatafact:
    def setup_method(self):
        # 各メソッドを実行する際に呼び出される初期化関数
        self.manager = DatafactManager()
        self.manager.update_results(datafact_2_1_1.subject, datafact_2_1_1.operation, result=5.309754398579871)
        self.manager.update_results(datafact_2_1_2.subject, datafact_2_1_2.operation, result=0.1977394782374222)
        self.manager.update_results(datafact_2_2_1.subject, datafact_2_2_1.operation, result=213.32)
        self.manager.update_results(datafact_2_2_2.subject, datafact_2_2_2.operation, result=29.99)
        self.manager.update_results(datafact_3_1_.subject, datafact_3_1_.operation, result=d)
    @pytest.mark.parametrize("datafact, df, expected_result", [
        # Aggregation
        (
            DATAFACT1_1,
            df,
            14.341366906474821,
        ),
        (
            DATAFACT1_2,
            df,
            3.3433617463315746,
        ),
        # Scalar Arithmetic
        (
            DATAFACT2_1,
            None,
            5.112014920342449
        ),
        (
            DATAFACT2_2,
            None,
            183.32999999999998
        ),
        # Rank
        (
            DATAFACT3,
            None,
            aa
        )
    ],
    ids = ["case1","case2","case3"]
    )
    def test_handle_datafact(self, datafact, expected_result):
        assert datafact.handle_datafact() == expected_result