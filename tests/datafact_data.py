import sys
sys.path.append('/Users/ashikawaharuki/Desktop/research/TDB/test/system/src')

# 自分で作成したクラス、関数をimport
from datafact_model import Datafact

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



"""
convert_datafact_to_operationflowのテスト用Datafact
"""
# 1. Aggregation
convert_datafact_1 = Datafact(
    subject=[{"Shipping Address State":"MA"}, "Category", ["ABIS_BOOK"]],
    operation = ["Aggregation", "Purchase Price Per Unit", "mean"]
)
convert_datafacts_1_1 = Datafact(
    subject=[{"Shipping Address State":"MA"}, "Category", ["*"]],
    operation = ["Aggregation", "Purchase Price Per Unit", "mean"]
)
convert_datafacts_1_2 = Datafact(
    subject=[{"Shipping Address State":"*"}, "Category", ["ABIS_BOOK"]],
    operation = ["Aggregation", "Purchase Price Per Unit", "mean"]
)
# 2. Aggregation→ScalarArithmetic
convert_datafact_2 = Datafact(
    subject=[{"Shipping Address State":"MA"}, "y", ["2022"]],
    operation=[
        "ScalarArithmetic", 
        "-",
        Datafact(
            subject=[{"Shipping Address State":"MA"},"y",[str(2022)]],
            operation=["Aggregation", "Purchase Price Per Unit", "mean"]
        ),
        Datafact(
            subject=[{"Shipping Address State":"MA"},"y",[str(2021)]],
            operation=["Aggregation", "Purchase Price Per Unit", "mean"]
        )
    ]
)
convert_datafacts_2_1 = Datafact(
    subject=[{"Shipping Address State":"MA"}, "y", ["*"]],
    operation=[
        "ScalarArithmetic", 
        "-",
        Datafact(
            subject=[{"Shipping Address State":"MA"},"y",["n"]],
            operation=["Aggregation", "Purchase Price Per Unit", "mean"]
        ),
        Datafact(
            subject=[{"Shipping Address State":"MA"},"y",["n-1"]],
            operation=["Aggregation", "Purchase Price Per Unit", "mean"]
        )
    ]
)
convert_datafacts_2_2 = Datafact(
    subject=[{"y":"*"}, "Shipping Address State", ["MA"]],
    operation=[
        "ScalarArithmetic", 
        "-",
        Datafact(
            subject=[{"y":"n"},"Shipping Address State",["MA"]],
            operation=["Aggregation", "Purchase Price Per Unit", "mean"]
        ),
        Datafact(
            subject=[{"y":"n-1"},"Shipping Address State",["MA"]],
            operation=["Aggregation", "Purchase Price Per Unit", "mean"]
        )
    ]
)
# 3. Aggregation→Rank
convert_datafact_3 = Datafact(
    subject=[{"Shipping Address State":"MA"}, "Category", ["ABIS_BOOK"]],
    operation=[
        "Rank",
        "降順",
        Datafact(
            subject=[{"Shipping Address State":"MA"}, "Category", ["*"]],
            operation = ["Aggregation", "Purchase Price Per Unit", "mean"]
        )
    ]
)
convert_datafacts_3_1 = Datafact(
    subject=[{"Shipping Address State":"MA"}, "Category", ["*"]],
    operation=[
        "Rank",
        "降順",
        Datafact(
            subject=[{"Shipping Address State":"MA"}, "Category", ["*"]],
            operation = ["Aggregation", "Purchase Price Per Unit", "mean"]
        )
    ]
)
convert_datafacts_3_2 = Datafact(
    subject=[{"Shipping Address State":"*"}, "Category", ["ABIS_BOOK"]],
    operation=[
        "Rank",
        "降順",
        Datafact(
            subject=[{"Shipping Address State":"*"}, "Category", ["ABIS_BOOK"]],
            operation = ["Aggregation", "Purchase Price Per Unit", "mean"]
        )
    ]
)
# 4. Aggregation→Scalar→Ran
convert_datafact_4 = Datafact(
    subject=[{"Shipping Address State":"MA"}, "y", ["2022"]],
    operation=[
        "Rank",
        "降順",
        Datafact(
            subject=[{"Shipping Address State":"MA"}, "y", ["*"]],
            operation=[
                "ScalarArithmetic", 
                "-",
                Datafact(
                    subject=[{"Shipping Address State":"MA"},"y",["n"]],
                    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
                ),
                Datafact(
                    subject=[{"Shipping Address State":"MA"},"y",["n-1"]],
                    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
                )
            ]
        )
    ]
)
convert_datafacts_4_1 = Datafact(
    subject=[{"Shipping Address State":"MA"}, "y", ["*"]],
    operation=[
        "Rank",
        "降順",
        Datafact(
            subject=[{"Shipping Address State":"MA"}, "y", ["*"]],
            operation=[
                "ScalarArithmetic", 
                "-",
                Datafact(
                    subject=[{"Shipping Address State":"MA"},"y",["n"]],
                    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
                ),
                Datafact(
                    subject=[{"Shipping Address State":"MA"},"y",["n-1"]],
                    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
                )
            ]
        )
    ]
)
convert_datafacts_4_2 = Datafact(
    subject=[{"y":"*"}, "Shipping Address State", ["MA"]],
    operation=[
        "Rank",
        "降順",
        Datafact(
            subject=[{"y":"*"}, "Shipping Address State", ["MA"]],
            operation=[
                "ScalarArithmetic", 
                "-",
                Datafact(
                    subject=[{"y":"n"},"Shipping Address State",["MA"]],
                    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
                ),
                Datafact(
                    subject=[{"y":"n-1"},"Shipping Address State",["MA"]],
                    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
                )
            ]
        )
    ]
)
# 5. Aggregation→Rank→ScalarArithmetic
convert_datafact_5 = Datafact(
    subject=[{"Shipping Address State":"MA"}, "y", ["2022"]],
    operation=[
        "ScalarArithmetic", 
        "-",
        Datafact(
            subject=[{"Shipping Address State":"MA"},"y",[str(2022)]],
            operation=[
                "Rank",
                "降順",
                Datafact(
                    subject=[{"Shipping Address State":"MA"},"y",["*"]],
                    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
                )
            ]
        ),
        Datafact(
            subject=[{"Shipping Address State":"MA"},"y",[str(2021)]],
            operation=[
                "Rank",
                "降順",
                Datafact(
                    subject=[{"Shipping Address State":"MA"},"y",["*"]],
                    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
                )
            ]
        ),
    ]
)
convert_datafacts_5_1 = Datafact(
    subject=[{"Shipping Address State":"MA"}, "y", ["*"]],
    operation=[
        "ScalarArithmetic", 
        "-",
        Datafact(
            subject=[{"Shipping Address State":"MA"},"y",["n"]],
            operation=[
                "Rank",
                "降順",
                Datafact(
                    subject=[{"Shipping Address State":"MA"},"y",["*"]],
                    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
                )
            ]
        ),
        Datafact(
            subject=[{"Shipping Address State":"MA"},"y",["n-1"]],
            operation=[
                "Rank",
                "降順",
                Datafact(
                    subject=[{"Shipping Address State":"MA"},"y",["*"]],
                    operation=["Aggregation", "Purchase Price Per Unit", "mean"]
                )
            ]
        ),
    ]
)