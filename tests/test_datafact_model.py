import unittest
# 自分で作成したクラス、関数をimport
from src.datafact_model import Datafact
from src.datafact_manager import DatafactManager


class TestDatafactManager(unittest.TestCase):
    def setUp(self):
        self.manager = DatafactManager()
    def test_makekey(self):
        # Aggregationのテスト
        datafact1 = Datafact(subject=[{"県":"東京都", "年":2022}, "大分類", ["製造業"]], operation=["Aggregation","売上","sum"])
        self.assert_equal(
            self.manager.make_key(subject=datafact1.subject,operation=datafact1.operation),
            (((("県","東京都"),("年",2022)), "大分類", ("製造業")), ("Aggregation", "売上高", "mean"))
        )



        