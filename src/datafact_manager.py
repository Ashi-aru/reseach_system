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
        self.templates = {}
        self.sentences = {}
        self.all_d = {
            "datafacts":self.datafacts,
            "results":self.results,
            "significances":self.significances,
            "templates":self.templates,
            "sentences":self.sentences
        }

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
    各データの保存・更新を行う関数。
    DatafactManager.results/significances/templates/sentencesに保存。キーはmake_keyで生成したもの。
    呼び出した際の可読性を向上させるために、関数名にはupdateする対象物(results/significances/..)を残している。
    入力:(subject, operation, update_data, data_type, makekey_flg)
    出力:None
    """
    def update_data(self, subject, operation, update_data, data_type, makekey_flg=True):
        data_d = self.all_d[data_type]
        if(makekey_flg):
            subject_key, operation_key = self.make_key(subject, operation)
        else:
            subject_key, operation_key = subject, operation

        if(subject_key not in data_d):
            data_d[subject_key] = {operation_key:update_data}
        else:
            data_d[subject_key][operation_key] = update_data
        # logging.info(self.significances[subject_key][operation_key])
        return None

    def update_results(self, subject, operation, result, makekey_flg=True):
        self.update_data(subject, operation, result, "results", makekey_flg)
        return None

    def update_significances(self, subject, operation, significance, makekey_flg=True):
        self.update_data(subject, operation, significance, "significances", makekey_flg)
        return None
    
    def update_templates(self, subject, operation, template, makekey_flg=True):
        self.update_data(subject, operation, template, "templates", makekey_flg)
        return None

    def update_sentences(self, subject, operation, sentence, makekey_flg=True):
        self.update_data(subject, operation, sentence, "sentences", makekey_flg)
        return None
    
    """
    datafacts,results,significancesに格納されているデータを検索し返す関数。
    入力:(subject, operation, selected_d_name)
        - selected_d: datafacts | results | significances
    出力:データ値
    """
    def search_value(self, subject, operation, selected_d_name):
        selected_d = self.all_d[selected_d_name]
        subject_key, operation_key = self.make_key(subject, operation)
        if((subject_key in selected_d) and (operation_key in selected_d[subject_key])):
            return selected_d[subject_key][operation_key]
        else:
            return None