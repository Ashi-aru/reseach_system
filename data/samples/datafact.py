import sys
sys.path.append('/Users/ashikawaharuki/Desktop/research/TDB/test/system/src')
# 自分で作成したクラス、関数をimport
from datafact_model import Datafact


ordinal_d = {"年":[2024, 2023, 2022, 2021, 2020, 2019, 2018]}


datafact1 = Datafact(
    subject = [{"大分類":"製造業","年":2022},"都道府県",["静岡"]],
    operation = [
        "ScalarArithmetic",
        "-",
        Datafact(
            subject = [{"大分類":"製造業","年":2022},"都道府県",["静岡"]],
            operation = [
                "Rank",
                "降順",
                Datafact(
                    subject = [{"大分類":"製造業","年":2022},"都道府県",["*"]],
                    operation = ["Aggregation", "売上", "sum"]
                )
            ]
        ),
        Datafact(
            subject = [{"大分類":"製造業","年":2021},"都道府県",["静岡"]],
            operation = [
                "Rank",
                "降順",
                Datafact(
                    subject = [{"大分類":"製造業","年":2021},"都道府県",["*"]],
                    operation = ["Aggregation", "売上", "sum"]
                )
            ]
        )
    ],
)

Datafact(
    subject=[{"大分類":"サービス業"}, "年", [2022]],
    operation=[
        "ScalarArithmetic", 
        "/",
        Datafact(
            subject=[{"大分類":"サービス業"}, "年", [2022]],
            operation=["Aggregation", "売上", "sum"]
        ),
        Datafact(
            subject=[{"大分類":"サービス業"}, "年", [2021]],
            operation=["Aggregation", "売上", "sum"]
        )
    ]
)

datafact3 = Datafact(
    subject=[{"大分類":"サービス業"}, "年", [2023]],
    operation=[
        "Rank",
        "降順",
        Datafact(
            subject=[{"大分類":"サービス業"}, "年", ["*"]],
            operation=[
                "ScalarArithmetic", 
                "-",
                Datafact(
                    subject=[{"大分類":"サービス業"},"年",["n"]],
                    operation=["Aggregation", "Purchase Price Per Unit", "sum"]
                ),
                Datafact(
                    subject=[{"大分類":"サービス業"},"年",["n-1"]],
                    operation=["Aggregation", "Purchase Price Per Unit", "sum"]
                )
            ]
        )
    ]
)