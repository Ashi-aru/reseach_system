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


PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/'data'

df = pd.read_csv(DATA_DIR/"tables/amazon-purchases.csv")
df["Order Date"] = pd.to_datetime(df["Order Date"])
df["y"] = df["Order Date"].dt.strftime("%Y")
df_MA = df[df["Shipping Address State"]=="MA"]

manager = DatafactManager()
ordinal_d = {"y":['2024', '2023', '2022', '2021', '2020', '2019', '2018']}
# datafactを事前に計算しておく
# Aggregationバージョン
# datafact1 → [{"Shipping Address State":"MA"}, "y", "*"]のパターン
# datafact2 → [{"y":"*"}, "Shipping Address State", "MA"]のパターン
result_d1 = {}
for y in range(2018, 2025):
    datafact1 = Datafact(
        subject=[{"Shipping Address State":"MA"},"y",[str(y)]],
        operation=["Aggregation", "Purchase Price Per Unit", "mean"]
    )
    datafact2 = Datafact(
        subject=[{"y":str(y)}, "Shipping Address State", ["MA"]],
        operation=["Aggregation", "Purchase Price Per Unit", "mean"]
    )
    df_y = df[df["y"]==str(y)]
    datafact1.handle_datafact(manager, df_MA)
    datafact2.handle_datafact(manager, df_y)
    result = manager.search_result(datafact1.subject, datafact1.operation)
    if(not np.isnan(result)): 
        result_d1[str(y)]=result
# ScalarArithmeticバージョン
# datafact1 → [{"Shipping Address State":"MA"}, "y", "*"]のパターン
# datafact2 → [{"y":"*"}, "Shipping Address State", "MA"]のパターン
result_d2 = {}
for y in range(2019, 2025):
    datafact1_1 = Datafact(
        subject=[{"Shipping Address State":"MA"},"y",[str(y)]],
        operation=["Aggregation", "Purchase Price Per Unit", "mean"]
    )
    datafact1_2 = Datafact(
        subject=[{"Shipping Address State":"MA"},"y",[str(y-1)]],
        operation=["Aggregation", "Purchase Price Per Unit", "mean"]
    )
    datafact1 = Datafact(
        subject=[{"Shipping Address State":"MA"},"y",[str(y)]],
        operation=["ScalarArithmetic", "-", datafact1_1, datafact1_2]
    )
    datafact2_1 = Datafact(
        subject=[{"y":str(y)}, "Shipping Address State", ["MA"]],
        operation=["Aggregation", "Purchase Price Per Unit", "mean"]
    )
    datafact2_2 = Datafact(
        subject=[{"y":str(y-1)}, "Shipping Address State", ["MA"]],
        operation=["Aggregation", "Purchase Price Per Unit", "mean"]
    )
    datafact2 = Datafact(
        subject=[{"y":str(y)}, "Shipping Address State", ["MA"]],
        operation=["ScalarArithmetic", "-", datafact2_1, datafact2_2]
    )
    datafact1.handle_datafact(manager)
    datafact2.handle_datafact(manager)
    result = manager.search_result(datafact1.subject, datafact1.operation)
    result_d2[str(y)]=result


DATAFACTs1 = Datafact(
    subject=[{"Shipping Address State":"MA"},"y",["*"]],
    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
)
DATAFACTs2 = Datafact(
    subject=[{"y":"*"}, "Shipping Address State", ["MA"]],
    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
)
datafact3_1 = Datafact(
    subject=[{"Shipping Address State":"MA"},"y",["n"]],
    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
)
datafact3_2 = Datafact(
    subject=[{"Shipping Address State":"MA"},"y",["n-1"]],
    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
)
DATAFACTs3 = Datafact(
    subject=[{"Shipping Address State":"MA"},"y",["*"]],
    operation=["ScalarArithmetic", "-", datafact3_1, datafact3_2]
)
datafact4_1 = Datafact(
    subject=[{"y":"n"}, "Shipping Address State", ["MA"]],
    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
)
datafact4_2 = Datafact(
    subject=[{"y":"n-1"},"Shipping Address State",["MA"]],
    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
)
DATAFACTs4 = Datafact(
    subject=[{"y":"*"},"Shipping Address State",["MA"]],
    operation=["ScalarArithmetic", "-", datafact4_1, datafact4_2]
)


class TestDatafact:
    def setup_method(self):
        # 各メソッドを実行する際に呼び出される初期化関数
        None
    @pytest.mark.parametrize("datafact, manager, df, ordinal_d, expected_result", [
        # Aggregation
        (
            DATAFACTs1,
            manager,
            df_MA,
            ordinal_d,
            result_d1,
        ),
        (
            DATAFACTs2,
            manager,
            df_MA,
            ordinal_d,
            result_d1,
        ),
        # Scalar Arithmetic
        (
            DATAFACTs3,
            manager,
            None,
            ordinal_d,
            result_d2
        ),
        (
            DATAFACTs4,
            manager,
            None,
            ordinal_d,
            result_d2
        ),
    ],
    ids = ["case1","case2","case3", "case4"]
    )
    def test_handle_datafacts(self, datafact, manager, df, ordinal_d, expected_result):
        datafact.handle_datafacts(manager, ordinal_d, df)
        result = manager.search_result(datafact.subject,datafact.operation)
        assert  result== expected_result