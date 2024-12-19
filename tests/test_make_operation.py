import pytest
import sys
import json

sys.path.append('/Users/ashikawaharuki/Desktop/research/TDB/test/system/src')

# 自分で作成したクラス、関数をimport
from datafact_model import Datafact
from datafact_manager import DatafactManager
from debug import debug_operation
from make_operation import make_operations, make_operations_for_datafacts
from logging_config import setup_logger
import datafact_data

ordinal_d = {"年":[2024, 2023, 2022, 2021, 2020, 2019, 2018]}
attr_type = {"大分類":"Categorical", "都道府県":"Categorical", "年":"Ordinal_t"}
looger = setup_logger()


class TestMakeOperation:
    def setup_method(self):
        # 各メソッドを実行する際に呼び出される初期化関数
        None
    
    @pytest.mark.parametrize("agg_attrs, agg_f_d, operator_d, subject, ordinal_d, step_n, attr_type", [
        # Aggregation
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,3],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",3):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"大分類":"製造業", "都道府県":"静岡県"}, "年", [2022]],
            ordinal_d,
            1,
            attr_type
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,3],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",3):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"大分類":"製造業", "都道府県":"静岡県"}, "年", [2022]],
            ordinal_d,
            2,
            attr_type
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,3],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",3):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"大分類":"製造業", "都道府県":"静岡県"}, "年", [2022]],
            ordinal_d,
            3,
            attr_type
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,3],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",3):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{}, "年", [2022]],
            ordinal_d,
            1,
            attr_type
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,3],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",3):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{}, "年", [2022]],
            ordinal_d,
            2,
            attr_type
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,3],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",3):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{}, "年", [2022]],
            ordinal_d,
            3,
            attr_type
        ),
    ],
    ids = [f'case{i}' for i in range(1,7)]
    )
    def test_make_operations(self, agg_attrs,agg_f_d, operator_d, subject, ordinal_d, step_n, attr_type):
        l = make_operations(agg_attrs,agg_f_d,operator_d,subject,ordinal_d,step_n, attr_type)
        for operation in l:
            looger.info(debug_operation(operation))

    @pytest.mark.parametrize("agg_attrs, agg_f_d, operator_d, subject, ordinal_d, attr_type", [
        # Scalarに突入しない
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,4],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",4):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"大分類":"製造業", "年":2022}, "都道府県", ["*"]],
            ordinal_d,
            attr_type
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,4],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",4):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"年":2022}, "都道府県", ["*"]],
            ordinal_d,
            attr_type
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,4],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",4):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{}, "都道府県", ["*"]],
            ordinal_d,
            attr_type
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,4],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",4):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"大分類":"製造業", "都道府県":"*"}, "年", [2022]],
            ordinal_d,
            attr_type
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,4],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",4):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"都道府県":"*"}, "年", [2022]],
            ordinal_d,
            attr_type
        ),
        # Scalarに突入
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,4],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",4):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"大分類":"製造業", "都道府県":"静岡県"}, "年", ["*"]],
            ordinal_d,
            attr_type
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,4],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",4):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"都道府県":"静岡県"}, "年", ["*"]],
            ordinal_d,
            attr_type
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,4],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",4):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{}, "年", ["*"]],
            ordinal_d,
            attr_type
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,4],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",4):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"大分類":"製造業", "年":"*"}, "都道府県", ["静岡"]],
            ordinal_d,
            attr_type
        ),
        (
            ["売上","従業員数","上場flg"],
            {"売上":[1,4],"従業員数":[2],"上場flg":[10]},
            {("売上",1):[2,4],("売上",4):[2,4],("従業員数",2):[2,4],("上場flg",10):[None]},
            [{"年":"*"}, "都道府県", ["静岡"]],
            ordinal_d,
            attr_type
        ),
    ],
    ids = [f'case{i}' for i in range(1,11)]
    )
    def test_make_operations_for_datafacts(self, agg_attrs,agg_f_d, operator_d, subject, ordinal_d, attr_type):
        l = make_operations_for_datafacts(agg_attrs,agg_f_d,operator_d,subject,ordinal_d,attr_type)
        for operation in l:
            looger.info(debug_operation(operation))