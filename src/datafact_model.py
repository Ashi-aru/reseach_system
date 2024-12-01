# 自分が定義したクラス、関数をインポート
from datafact_manager import DatafactManager

class Datafact:
    manager = DatafactManager()
    """
    - datafact.subject = [{"県":"東京都", "年":2022}, "大分類", ["製造業"]]
    - datafact.operation = ["Aggregation", "売上高", "mean"]
    """
    def __init__(self, subject, operation):
        self.subject = subject
        self.operation = operation