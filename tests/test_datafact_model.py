import pytest
# 自分で作成したクラス、関数をimport
from src.datafact_model import Datafact
from src.datafact_manager import DatafactManager


class TestDatafactManager:
    def setup_method(self):
        # 各メソッドを実行する際に呼び出される初期化関数
        self.manager = DatafactManager()
    
    @pytest.mark.parametrize("datafact, expected_result", [
        # Aggregation
        (
            Datafact(
                subject=[{"県":"東京都", "年":2022}, "大分類", ["製造業"]],
                operation=["Aggregation", "売上高", "mean"]
            ),
            (
                ((("県","東京都"),("年",2022)), "大分類", ("製造業")), # subjectのexpected_result
                ("Aggregation", "売上高", "mean") # operationのexpected_result
            ),
        ),
        # Scalar Arithmetic
        (
            Datafact(
                subject=[{"県":"東京都", "年":2022}, "大分類", ["製造業"]],
                operation=[
                    "scalarArithmetic", 
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
            ),
            (
                ((("県","東京都"),("年",2022)), "大分類", ("製造業")), # ←subject ↓operation
                (
                    "scalarArithmetic", 
                    "-",
                    (
                        ((("県","東京都"),("年",2022)), "大分類", ("製造業")), # datafact1のsubject
                        ("Aggregation", "売上高", "mean") # datafact1のoperation
                    ),
                    (
                        ((("県","東京都"),("年",2022)), "大分類", ("サービス業")), # datafact2のsubject
                        ("Aggregation", "売上高", "mean") # datafact2のoperation
                    )
                )
            )
        ),
        # Rank
        (
            Datafact(
                subject=[{"県":"東京都", "年":2022}, "大分類", ["製造業"]],
                operation=[
                    "rank",
                    "降順",
                    Datafact(
                        subject=[{"県":"東京都", "年":"*"}, "大分類", ["製造業"]],
                        operation = [
                            "scalarArithmetic", 
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
            ),
            (
                ((("県","東京都"),("年",2022)), "大分類", ("製造業")), # ←subject ↓operation
                (
                    "rank",
                    "降順",
                    (
                        ((("県","東京都"),("年","*")), "大分類", ("製造業")),
                        (
                            "scalarArithmetic", 
                            "-",
                            (
                                ((("県","東京都"), ("年","n")), "大分類", ("製造業")),
                                ("Aggregation", "売上高", "mean")
                            ),
                            (
                                (("県","東京都"), ("年","n-1"), "大分類", ("製造業")),
                                ("Aggregation", "売上高", "mean")
                            ),
                        )
                    )
                )
            )
        )
    ])
    def test_makekey(self, datafact, expected_result):
        subject, operation = datafact.subject, datafact.operation
        assert self.manager.make_key(subject=subject,operation=operation) == expected_result



        