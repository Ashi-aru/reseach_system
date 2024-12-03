import operator
import logging
from pathlib import Path
from datetime import date
# 自分が定義したクラス、関数をインポート
from datafact_manager import DatafactManager


PROJ_DIR = Path(__file__).resolve().parent.parent
TODAY = date.today().strftime("%Y-%m-%d")

logging.basicConfig(
        level=logging.INFO,
        filename=PROJ_DIR/f'log/datafact_manager/{TODAY}.log',
        format='%(asctime)s\n%(message)s'
    )


class Datafact:
    """
    - datafact.subject = [{"県":"東京都", "年":2022}, "大分類", ["製造業"]]
    - datafact.operation = ["Aggregation", "売上高", "mean"]
    """
    def __init__(self, subject, operation, ):
        self.subject = subject
        self.operation = operation

    
    # operationを実行し、結果をresultインスタンスに保存
    """
    ドリルダウンの木構造における、ノードの計算を実施する関数
    """
    def handle_datafact(self, manager, df=None):
        parents, column_name, filter_values = self.subject
        operation_name, *operation_others = self.operation
    
        if(operation_name=="Aggregation"):
            if(df is not None):
                df_filtered = df[df[column_name].isin(filter_values)]
            # Operation:["Aggregation", カラム名, Aggregation_Func]
            aggregation_col, f_name = operation_others
            s, s_filtered = df[aggregation_col], df_filtered[aggregation_col]
            f = {
                "sum": (lambda s_filtered,_:s_filtered.sum()),
                "sum_percent": (lambda s_filtered,s:(s_filtered.sum()/s.sum())*100),
                "mean": (lambda s_filtered,_:s_filtered.mean()),
                # "mean_percent": (lambda s_filtered,s:(s_filtered.mean()/s.mean())*100),
                "max": (lambda s_filtered,_:s_filtered.max()),
                "min": (lambda s_filtered,_:s_filtered.min()),
                "median": (lambda s_filtered,_:s_filtered.median()),
                "count": (lambda s_filtered,_:s_filtered.count()),
                "count_percent": (lambda s_filtered,s:(s_filtered.count()/s.count())*100),
                "nunique": (lambda s_filtered,_:s_filtered.nunique()),
                "unique": (lambda s_filtered,_:s_filtered.unique())
            }
            result = f[f_name](s_filtered, s)
            # logging.info(result)
            manager.update_results(self.subject,self.operation,result=result)

        elif(operation_name=="ScalarArithmetic"):
            # Operation:["ScalarArithmetic", 演算子, datafact1, datafact2]
            op, datafact1, datafact2 = operation_others
            result1 = manager.search_value(datafact1.subject, datafact1.operation, "results")
            result2 = manager.search_value(datafact2.subject, datafact2.operation, "results")
            operators = {"+": operator.add,"-": operator.sub,"*": operator.mul,"/": operator.truediv}
            result = operators[op](result1, result2)
            # logging.info(result)
            manager.update_results(self.subject,self.operation,result=result)
        # TODO: 同率順位の時の対応
        elif(operation_name=="Rank"):
            # Operation:["Rank", 昇順or降順, datafacts（群）]
            # datafacts（群）:[[{"k":"v"}, "brabra", ["*"]], 演算内容] or [[{"brabra":"*"}, "k", ["v"]], 演算内容]
            order, datafacts = operation_others
            subject2, operation2 = datafacts.subject, datafacts.operation
            ranks_d = manager.search_value(subject2, self.operation, "results")
            if(ranks_d is not None):
                # logging.info(ranks_d[filter_values[0]])
                manager.update_results(self.subject,self.operation,result=ranks_d[filter_values[0]])
            else:
                results_d = manager.search_value(subject2,operation2,"results")
                if(results_d is not None):
                    # results_dは{"k1":v1,"k2":v2,...}という形を想定
                    order = True if(order=="降順") else False
                    sorted_results_d = dict(sorted(results_d.items(), key=lambda item: item[1], reverse=order))
                    ranks_d = dict([(key, i) for i, key in enumerate(list(sorted_results_d.keys()))])
                    # 次に備えて、ranks_dは保存しておく
                    manager.update_results(subject2, self.operation,result=ranks_d)
                    manager.update_results(self.subject,self.operation,result=ranks_d[filter_values[0]])
                    # logging.info(ranks_d[filter_values[0]])
                else:
                    raise ValueError("Rankをつける値の計算をまだしてないんじゃないか！！")
        else:
            raise ValueError("サポートしていないOperation名を書くな！")
    
