import pandas as pd
import logging
from pathlib import Path
from datetime import date


PROJ_DIR = Path(__file__).resolve().parent.parent
TODAY = date.today().strftime("%Y-%m-%d")

logging.basicConfig(
        level=logging.INFO,
        filename=PROJ_DIR/f'log/datafact_manager/{TODAY}.log',
        format='%(asctime)s\n%(message)s'
    )


class DatafactManager:
    def __init__(self):
        self.datafacts = {}
        self.results = {}
        self.significances = {}

    """
    results, significancesなどを保存するキーを生成を行う関数。ハッシュ不可能なデータ型をハッシュ可能データがに変更。
    入力:
    - datafact.subject = [{"県":"東京都", "年":2022}, "大分類", ["製造業"]]
    - datafact.operation = ["Aggregation", "売上高", "mean"]
    出力:
    - subject_key = ((("県","東京都"),("年",2022)), "大分類", ("製造業"))
    - operation_key = ("Aggregation", "売上高", "mean")
    """
    def make_key(self, subject, operation):
        subject_key = (tuple([(k,v) for k,v in subject[0].items()]), subject[1], (subject[2][0],))

        operation_name, *operation_others = operation
        # logging.info(operation_others)

        if(operation_name=="Aggregation"):
            culumn_name, f_name = operation_others
            operation_key = tuple([operation_name, culumn_name, f_name])
        elif(operation_name=="ScalarArithmetic"):
            operator, datafact1, datafact2 = operation_others
            operation_key = tuple([operation_name, operator, self.make_key(datafact1.subject, datafact1.operation), self.make_key(datafact2.subject, datafact2.operation)])
        elif(operation_name=="Rank"):
            order, datafact = operation_others
            operation_key = tuple([operation_name, order, self.make_key(datafact.subject, datafact.operation)])
        else:
            datafact = operation_others[0]
            operation_key = tuple([operation_name, self.make_key(datafact.subject, datafact.operation)])
        return (subject_key, operation_key)
    
    """
    計算結果の保存・更新を行う関数。
    DatafactManager.resultsに保存。キーはmake_keyで生成したもの。
    つまり、results[subject_key][operation_key]=resultという形。
    入力:(subject, operation, result)
    出力:None
    """
    def update_results(self, subject, operation, result, makekey_flg=True):
        if(makekey_flg):
            subject_key, operation_key = self.make_key(subject, operation)
        else:
            subject_key, operation_key = subject, operation

        if(subject_key not in self.results):
            self.results[subject_key] = {operation_key:result}
        else:
            self.results[subject_key][operation_key] = result
        logging.info(self.results[subject_key][operation_key])
        return None

