import pytest
import sys
sys.path.append('/Users/ashikawaharuki/Desktop/research/TDB/test/system/src')

# 自分で作成したクラス、関数をimport
from datafact_model import Datafact
from datafact_manager import DatafactManager

DATAFACT1 = Datafact(
                subject=[{"県":"東京都", "年":2022}, "大分類", ["製造業"]],
                operation=["Aggregation", "売上高", "mean"]
            )
DATAFACT2 = Datafact(
                subject=[{"県":"東京都", "年":2022}, "大分類", ["製造業"]],
                operation=[
                    "ScalarArithmetic", 
                    "-",
                    Datafact(
                        subject=[{"県":"東京都", "年":2022}, "大分類", ["製造業"]],
                        operation=["Aggregation", "売上高", "mean"]
                    ),
                    Datafact(
                        subject=[{"県":"東京都", "年":2022}, "大分類", ["サービス業"]],
                        operation=["Aggregation", "売上高", "mean"]
                    ),
                ]
            )
DATAFACT3 = Datafact(
                subject=[{"県":"東京都", "年":2022}, "大分類", ["製造業"]],
                operation=[
                    "Rank",
                    "降順",
                    Datafact(
                        subject=[{"県":"東京都", "年":"*"}, "大分類", ["製造業"]],
                        operation = [
                            "ScalarArithmetic", 
                            "-",
                            Datafact(
                                subject=[{"県":"東京都", "年":"n"}, "大分類", ["製造業"]],
                                operation=["Aggregation", "売上高", "mean"]
                            ),
                            Datafact(
                                subject=[{"県":"東京都", "年":"n-1"}, "大分類", ["製造業"]],
                                operation=["Aggregation", "売上高", "mean"]
                            ),
                        ]
                    )
                ]
            )

class TestDatafactManager:
    def setup_method(self):
        # 各メソッドを実行する際に呼び出される初期化関数
        self.manager = DatafactManager()
    
    @pytest.mark.parametrize("datafact, expected_result", [
        # Aggregation
        (
            DATAFACT1,
            (
                ((("県","東京都"),("年",2022)), "大分類", ("製造業",)), # subjectのexpected_result
                ("Aggregation", "売上高", "mean") # operationのexpected_result
            ),
        ),
        # Scalar Arithmetic
        (
            DATAFACT2,
            (
                ((("県","東京都"),("年",2022)), "大分類", ("製造業",)), # ←subject ↓operation
                (
                    "ScalarArithmetic", 
                    "-",
                    (
                        ((("県","東京都"),("年",2022)), "大分類", ("製造業",)), # datafact1のsubject
                        ("Aggregation", "売上高", "mean") # datafact1のoperation
                    ),
                    (
                        ((("県","東京都"),("年",2022)), "大分類", ("サービス業",)), # datafact2のsubject
                        ("Aggregation", "売上高", "mean") # datafact2のoperation
                    )
                )
            )
        ),
        # Rank
        (
            DATAFACT3,
            (
                ((("県","東京都"),("年",2022)), "大分類", ("製造業",)), # ←subject ↓operation
                (
                    "Rank",
                    "降順",
                    (
                        ((("県","東京都"),("年","*")), "大分類", ("製造業",)),
                        (
                            "ScalarArithmetic", 
                            "-",
                            (
                                ((("県","東京都"), ("年","n")), "大分類", ("製造業",)),
                                ("Aggregation", "売上高", "mean")
                            ),
                            (
                                ((("県","東京都"), ("年","n-1")), "大分類", ("製造業",)),
                                ("Aggregation", "売上高", "mean")
                            ),
                        )
                    )
                )
            )
        )
    ],
    ids = ["case1","case2","case3"]
    )
    def test_makekey(self, datafact, expected_result):
        subject, operation = datafact.subject, datafact.operation
        assert self.manager.make_key(subject=subject,operation=operation) == expected_result

    @pytest.mark.parametrize("datafact, result, expected_result", [
        (
            DATAFACT1,
            15,
            None
        ),
        (
            DATAFACT3,
            3,
            None
        )
    ])
    def test_update_results(self, datafact, result, expected_result):
        subject, operation = datafact.subject, datafact.operation
        assert self.manager.update_results(subject=subject, operation=operation, result=result) == expected_result

    @pytest.mark.parametrize("datafact, significance, expected_result", [
        (
            DATAFACT1,
            5.6,
            None
        ),
        (
            DATAFACT3,
            2.5,
            None
        )
    ])
    def test_update_significances(self, datafact, significance, expected_result):
        subject, operation = datafact.subject, datafact.operation
        assert self.manager.update_significances(subject=subject, operation=operation, significance=significance) == expected_result
    
    @pytest.mark.parametrize("boolean, datafact, result, selected_d_name, expected_result", [
        (True,DATAFACT1,5.6,"results",5.6),
        (False,DATAFACT2,2.5,"results",None),
        (True,DATAFACT1,5.6,"significances",5.6),
        (False,DATAFACT2,2.5,"results",None),
    ])
    def test_search_value(self, boolean, datafact, result, selected_d_name, expected_result):
        subject, operation = datafact.subject, datafact.operation
        if(boolean):
            if(selected_d_name=="results"):
                self.manager.update_results(subject, operation, result)
            elif(selected_d_name=="significances"):
                self.manager.update_significances(subject, operation, result)
        
        assert self.manager.search_value(subject, operation, selected_d_name) == expected_result