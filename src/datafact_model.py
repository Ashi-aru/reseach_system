import operator
import logging
from pathlib import Path
from datetime import date
import json
import re
import copy
# 自分が定義したクラス、関数をインポート
from datafact_manager import DatafactManager
from operation2language import Aggregation, AttributeArithmetic, AttributeScalarArithmetic, AttributeSelection, Difference, GroupingOperation, ItemFiltering, ScalarArithmetic, Shift, Sort ,Sort_ordinal_d, ValueSelection, generate_IF_request
from others import is_datafacts as Is_datafacts


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
    入力:
    - manager: DatafactManagerインスタンス(計算結果を保存したり、取り出したり)
    - df: dataframe
    出力:
    - None(計算結果はDatafactManager.resultsに保存)
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
            result1 = manager.search_result(datafact1.subject, datafact1.operation)
            result2 = manager.search_result(datafact2.subject, datafact2.operation)
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
            ranks_d = manager.search_result(subject2, self.operation)
            if(ranks_d is not None):
                # logging.info(ranks_d[filter_values[0]])
                manager.update_results(self.subject,self.operation,result=ranks_d[filter_values[0]])
            else:
                results_d = manager.search_result(subject2,operation2)
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
    
    """
    datafactsについての計算を行う関数。つまり、各ノードの計算結果をグループごとにまとめる関数
    出力
    - None(計算結果はresult_dをDatafactManager.resultsに保存)
        - result_d 例: {2021:v1, 2022:v2, ...}
    """
    def handle_datafacts(self, manager, ordinal_d=None, df=None):
        parents, column_name, filter_values = self.subject
        operation_name, *operation_others = self.operation
        # Aggregation,Rankでは、Operationは全てのデータファクトで共通となる。
        if(operation_name=="Aggregation" or operation_name=="Rank"):
            if(df is None):
                raise ValueError("dfが必要なのに引数に指定されていません！")
            if(filter_values==["*"]):
                result_d = dict([
                    (fv,manager.search_result([parents, column_name, [fv]], self.operation)) 
                    for fv in df[column_name].unique()
                ])
            else:
                for k, v in parents.items():
                    if(v=="*"): 
                        sig_key = k
                result_d = {}
                for v_tmp in df[sig_key].unique():
                    n_parents = copy.deepcopy(parents)
                    n_parents[sig_key] = v_tmp
                    result_d[v_tmp] = manager.search_result([n_parents, column_name, filter_values], self.operation)
            manager.update_results(self.subject, self.operation, result=result_d)
        # ScalarArithmeticだけは少し特殊
        elif(operation_name=="ScalarArithmetic"):
            operator, datafact1, datafact2 = operation_others
            result_d = {}
            if(ordinal_d is None):
                raise ValueError("ordinal_dが必要なのに引数に指定されていません！")
            if(filter_values==["*"]):
                parents_, column_name_, _ = datafact1.subject
                parents_, column_name_, _ = datafact2.subject
                for i in range(len(ordinal_d[column_name_])-1):
                    n_datafact1 = Datafact(subject=[parents_, column_name_, [ordinal_d[column_name_][i]]], operation=datafact1.operation)
                    n_datafact2 = Datafact(subject=[parents_, column_name_, [ordinal_d[column_name_][i+1]]], operation=datafact2.operation)
                    n_operation = ["ScalarArithmetic", operator, n_datafact1, n_datafact2]
                    result_d[ordinal_d[column_name_][i]] = manager.search_result(n_datafact1.subject, n_operation)
                    
            else:
                _, column_name_, filter_values_ = datafact1.subject
                _, column_name_, filter_values_ = datafact2.subject
                for k, v in parents.items():
                    if(v=="*"): 
                        sig_key = k
                for i in range(len(ordinal_d[sig_key])-1):
                    n_parents1 = copy.deepcopy(parents)
                    n_parents1[sig_key] = ordinal_d[sig_key][i]
                    n_datafact1 = Datafact(subject=[n_parents1, column_name_, filter_values_], operation=datafact1.operation)
                    
                    n_parents2 = copy.deepcopy(parents)
                    n_parents2[sig_key] = ordinal_d[sig_key][i+1]
                    n_datafact2 = Datafact(subject=[n_parents2, column_name_, filter_values_], operation=datafact2.operation)

                    n_operation = ["ScalarArithmetic", operator, n_datafact1, n_datafact2]
                    result_d[ordinal_d[sig_key][i]] = manager.search_result(n_datafact1.subject, n_operation)
            manager.update_results(self.subject, self.operation, result=result_d)
        # TODO:残りの三つの操作に対応
        elif(operation_name in ["Trend","Extreme","Outlier"]):
            return None
        else:
            raise ValueError("登録されていないOperation名です！")

    """
    データファクトを受け取り、データ操作の言語化を行い、保存・出力を行う関数
    入力: datafact(self)
    出力: データ操作フローを言語化したもの(辞書)
    """
    # NOTE: Rank,ScalarArithmeticの時は、ordinal_dは常に引数に入れるのが無難かも
    def convert_datafact_to_operationflow(self, ordinal_d=None):
        def numbering(n,k):
            return f"{n}.{k}" if(n!="t") else f"t{k}"
        
        def datafact2flow_d(self=self, ordinal_d=ordinal_d, n="t"):
            parents, col_name, filter_values = self.subject
            operation_name, *operation_others = self.operation
            is_datafacts, key_attr = Is_datafacts(subject=self.subject)
            flow_d = {}
            if(operation_name == "Aggregation"):
                aggregation_col, f_name = operation_others
                if(is_datafacts):
                    flow_d[numbering(n,1)]=ItemFiltering(generate_IF_request(self.subject))
                    flow_d[numbering(n,2)]=GroupingOperation([f_name,[col_name],[aggregation_col]])
                else:
                    flow_d[numbering(n,1)]=ItemFiltering(generate_IF_request(self.subject))
                    flow_d[numbering(n,2)]=Aggregation([aggregation_col, f_name])
                return flow_d

            elif(operation_name == "ScalarArithmetic"):
                op, datafact1, datafact2 = operation_others
                if(is_datafacts):
                    """
                    Aggregation,Rankのdatafactsの対応 → Ordinal Attributeの順に並び替え → 上に1つ分シフトした列を生成 → AttributeArithmetic
                    """
                    if(ordinal_d is None):
                        raise ValueError("ordinal_dが必要なのに引数に指定されていません！")
                    flow_d[numbering(n,1)]=datafact2flow_d(datafact1,ordinal_d,n=numbering(n,1))
                    flow_d[numbering(n,2)]=Sort_ordinal_d([numbering(n,1),key_attr, ordinal_d])
                    flow_d[numbering(n,3)]=Shift([numbering(n,2),1])
                    flow_d[numbering(n,4)]=AttributeArithmetic([op, [numbering(n,2), numbering(n,3)]])
                else:
                    flow_d[numbering(n,1)]=datafact2flow_d(datafact1,n=numbering(n,1))
                    flow_d[numbering(n,2)]=datafact2flow_d(datafact2,n=numbering(n,2))
                    flow_d[numbering(n,3)]=ScalarArithmetic([op,numbering(n,1),numbering(n,2)])
                return flow_d
        
            elif(operation_name == "Rank"):
                order, datafacts = operation_others
                flow_d[numbering(n,1)]=datafact2flow_d(datafacts,ordinal_d,n=numbering(n,1))
                flow_d[numbering(n,2)]=Sort([numbering(n,1), order])
                if(not is_datafacts):
                    flow_d[numbering(n,3)]=ValueSelection([[col_name, filter_values[0]], "順位"])
                return flow_d
            else:
                raise ValueError("登録されていないOperation名です！")
        flow_d = datafact2flow_d()
        logging.info(json.dumps(flow_d,ensure_ascii=False,indent=4))
        return flow_d

