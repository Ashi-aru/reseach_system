import pytest
import sys
import pandas as pd
from pathlib import Path
import random

sys.path.append('/Users/ashikawaharuki/Desktop/research/TDB/test/system/src')

# 自分で作成したクラス、関数をimport
from datafact_model import Datafact
from datafact_manager import DatafactManager

PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/'data'

df = pd.read_csv(DATA_DIR/"tables/amazon-purchases.csv")
df_MA = df[df["Shipping Address State"]=="MA"]
s_unique = df_MA["Category"].unique()
num_l = [i for i in range(1, len(s_unique)+1)]
random.shuffle(num_l)
rank_d_1 = dict([(s_unique[i-1], num_l[i-1]) for i in range(1, len(s_unique)+1)])

grouped = df_MA.groupby('Category')["Purchase Price Per Unit"].sum()
value_d = grouped.to_dict()
sorted_values_d = dict(sorted(value_d.items(), key=lambda item: item[1], reverse=True))
rank_d_2 = dict([(key, i) for i, key in enumerate(list(sorted_values_d.keys()))])


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
DATAFACT3_1 = Datafact(
                subject=[{"Shipping Address State":"MA"}, "Category", ["ABIS_BOOK"]],
                operation=[
                    "Rank",
                    "降順",
                    datafact_3_1_
                ]
            )
datafact_3_2_ = Datafact(
                        subject=[{"Shipping Address State":"MA"}, "Category", ["*"]],
                        operation = ["Aggregation", "Purchase Price Per Unit", "sum"]
                    )
DATAFACT3_2 = Datafact(
                subject=[{"Shipping Address State":"MA"}, "Category", ["ABIS_BOOK"]],
                operation=[
                    "Rank",
                    "降順",
                    datafact_3_2_
                ]
            )

manager = DatafactManager()
manager.update_results(datafact_2_1_1.subject, datafact_2_1_1.operation, result=5.309754398579871)
manager.update_results(datafact_2_1_2.subject, datafact_2_1_2.operation, result=0.1977394782374222)
manager.update_results(datafact_2_2_1.subject, datafact_2_2_1.operation, result=213.32)
manager.update_results(datafact_2_2_2.subject, datafact_2_2_2.operation, result=29.99)
manager.update_results(datafact_3_1_.subject, DATAFACT3_1.operation, result=rank_d_1)
manager.update_results(datafact_3_2_.subject, datafact_3_2_.operation, result=value_d)

class TestDatafact:
    def setup_method(self):
        # 各メソッドを実行する際に呼び出される初期化関数
        None
        
    @pytest.mark.parametrize("datafact, manager, df, expected_result", [
        # Aggregation
        (
            DATAFACT1_1,
            manager,
            df_MA,
            14.341366906474821,
        ),
        (
            DATAFACT1_2,
            manager,
            df_MA,
            3.3433617463315746,
        ),
        # Scalar Arithmetic
        (
            DATAFACT2_1,
            manager,
            None,
            5.112014920342449
        ),
        (
            DATAFACT2_2,
            manager,
            None,
            183.32999999999998
        ),
        # Rank
        (
            DATAFACT3_1,
            manager,
            None,
            rank_d_1["ABIS_BOOK"]
        ),
        (
            DATAFACT3_2,
            manager,
            None,
            rank_d_2["ABIS_BOOK"]
        )
    ],
    ids = ["case1","case2","case3", "case4", "case5", "case6"]
    )
    def test_handle_datafact(self, datafact, manager, df, expected_result):
        datafact.handle_datafact(manager, df)
        result = manager.search_value(datafact.subject,datafact.operation,"results")
        assert  result== expected_result