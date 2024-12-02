import operator
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

    
    # operationを実行し、結果をresultインスタンスに保存
    """
    ドリルダウンの木構造における、ノードの計算を実施する関数
    """
    def handle_datafact(self, df=None):
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
            Datafact.manager.update_results(self.subject,self.operation,result=result)
        elif(operation_name=="ScalarArithmetic"):
            # Operation:["ScalarArithmetic", 演算子, datafact1, datafact2]
            op, datafact1, datafact2 = operation_others
            result1 = Datafact.manager.search_value(datafact1.subject, datafact1.operation, "results")
            result2 = Datafact.manager.search_value(datafact2.subject, datafact2.operation, "results")
            operators = {"add": operator.add,"sub": operator.sub,"mul": operator.mul,"truediv": operator.truediv}
            result = operators[op](result1, result2)
            Datafact.manager.update_results(self.subject,self.operation,result=result)
        elif(operation_name=="Rank"):
            # Operation:["Rank", 昇順or降順, datafacts（群）]
            # datafacts（群）:[[{"k":"v"}, "brabra", ["*"]], 演算内容] or [[{"brabra":"*"}, "k", ["v"]], 演算内容]
            order, datafacts = operation_others
            subject2, operation2 = datafacts
            rank_results_d = Datafact.manager.search_result(subject2, self.operation, "results")
            if(rank_results_d is not None):
                Datafact.manager.update_results(self.subject,self.operation,result=rank_results_d[filter_values[0]])
            else:
                operation2_results_d = Datafact.manager.search_result(subject2,operation2,"results")
                if(operation2_results_d is not None):
                    # result2は{"k1":v1,"k2":v2,...}という形を想定
                    order = True if(order=="降順") else False
                    sorted_operation2_results = dict(sorted(result2.items(), key=lambda item: item[1], reverse=order))
                    rank_results_d = dict([(key, i) for i, key in enumerate(list(sorted_operation2_results.keys()))])
                    # 次に備えて、rank_results_dは保存しておく
                    Datafact.manager.update_results(subject2, self.operation,result=rank_results_d)
                    Datafact.manager.update_results(self.subject,self.operation,result=rank_results_d[filter_values[0]])
                else:
                    raise ValueError("Rankをつける値の計算をまだしてないんじゃないか！！")
        else:
            raise ValueError("サポートしていないOperation名を書くな！")
    
