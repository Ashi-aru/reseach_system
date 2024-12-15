import pytest
import sys
import json

sys.path.append('/Users/ashikawaharuki/Desktop/research/TDB/test/system/src')

# 自分で作成したクラス、関数をimport
from datafact_model import Datafact
from datafact_manager import DatafactManager
from debug import debug_operation
from make_operation import make_operations
from logging_config import setup_logger
import datafact_data

ordinal_d = {"年":['2024', '2023', '2022', '2021', '2020', '2019', '2018']}
looger = setup_logger()


class TestMakeOperation:
    def setup_method(self):
        # 各メソッドを実行する際に呼び出される初期化関数
        None
    
    @pytest.mark.parametrize("agg_attrs, agg_f_d, operator_d, subject, ordinal_d, step_n", [
        # Aggregation
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,3],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",3):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"大分類":"製造業", "都道府県":"静岡県"}, "年", ["2022"]],
            ordinal_d,
            1,
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,3],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",3):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"大分類":"製造業", "都道府県":"静岡県"}, "年", ["2022"]],
            ordinal_d,
            2
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,3],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",3):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"大分類":"製造業", "都道府県":"静岡県"}, "年", ["2022"]],
            ordinal_d,
            3
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,3],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",3):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{}, "年", ["2022"]],
            ordinal_d,
            1,
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,3],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",3):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{}, "年", ["2022"]],
            ordinal_d,
            2
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,3],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",3):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{}, "年", ["2022"]],
            ordinal_d,
            3
        ),
    ],
    ids = [f'case{i}' for i in range(1,7)]
    )
    def test_make_operations(self, agg_attrs,agg_f_d, operator_d, subject, ordinal_d, step_n):
        l = make_operations(agg_attrs,agg_f_d,operator_d,subject,ordinal_d,step_n)
        for operation in l:
            looger.info(debug_operation(operation))

