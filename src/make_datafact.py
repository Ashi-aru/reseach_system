from pathlib import Path
from datetime import date
import copy
# 自分で定義した関数・クラスをimport
from logging_config import setup_logger
from datafact_model import Datafact


PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/"data"
TODAY = date.today().strftime("%Y-%m-%d")

logger = setup_logger()

"""
datafactを受け取り、それが含まれるdatafactsのリストを返す関数
【入力】
- datafact: Datafactオブジェクト
【出力】
- datafacts_l: datafactが含まれるdatafactsのリスト
"""
def datafact2datafacts(datafact):
    datafacts_l = []
    parents, column_name, filter_values = datafact.subject
    operation_name, *operation_others = datafact.operation
    if(operation_name=="Aggregation"):
        datafacts1 = Datafact(
            subject = [parents, column_name, ['*']],
            operation = datafact.operation
        )
        datafacts_l.append(datafacts1)
        if(parents!={}):
            n_parents = {}
            for i, (k,v) in enumerate(parents.items()):
                if(i==len(parents)-1):
                    n_parents[k] = '*'
                else:
                    n_parents[k] = v
            datafacts2 = Datafact(
                subject = [n_parents, column_name, filter_values],
                operation = datafact.operation
            )
            datafacts_l.append(datafacts2)
    elif(operation_name=="ScalarArithmetic"):
        op, datafact1, datafact2 = operation_others
        parents1, column_name1, fliter_values1 = datafact1.subject
        operation_name1, *operation_others1 = datafact1.operation
        parents2, column_name2, fliter_values2 = datafact2.subject
        if(operation_name1=="Aggregation"):
            datafact1 = Datafact(
                subject = [parents1, column_name1, ['n']],
                operation = datafact1.operation
            )
            datafact2 = Datafact(
                subject = [parents2, column_name2, ['n-1']],
                operation = datafact2.operation
            )
            datafacts1 = Datafact(
                subject = [parents, column_name, ['*']],
                operation = ["ScalarArithmetic", op, datafact1, datafact2]
            )
            datafacts_l.append(datafacts1)
            if(parents!={}):
                n_parents = {}
                for i, (k,v) in enumerate(parents.items()):
                    if(i==len(parents)-1):
                        n_parents[k] = '*'
                    else:
                        n_parents[k] = v
                datafacts2 = Datafact(
                    subject = [n_parents, column_name, filter_values],
                    operation = [
                        "ScalarArithmetic", 
                        op, 
                        Datafact(
                            subject = [n_parents, column_name1, fliter_values1],
                            operation = datafact1.operation
                        ), 
                        Datafact(
                            subject = [n_parents, column_name2, fliter_values2],
                            operation = datafact2.operation
                        )
                    ]
                )
                datafacts_l.append(datafacts2)
        else:
            pass
    elif(operation_name=="Rank"):
        order, datafacts1 = operation_others
        operation_name1, *operation_others1 = datafacts1.operation
        if(operation_name1=="Aggregation"):
            datafacts = Datafact(
                subject=[parents, column_name, ['*']],
                operation=[
                    'Rank',
                    order,
                    Datafact(
                        subject=[parents, column_name, ['*']],
                        operation=datafacts1.operation
                    )
                ]
            )
            datafacts_l.append(datafacts)
        elif(operation_name1=='ScalarArithmetic'):
            op, datafact1_1, datafact1_2 = operation_others1
            parents1_1, column_name1_1, _ = datafact1_1.subject
            parents1_2, column_name1_2, _ = datafact1_2.subject
            operation_name1_1, *operation_others1_1 = datafact1_1.operation
            if(operation_name1_1=="Aggregation"):
                datafact1_1 = Datafact(
                    subject = [parents1_1, column_name1_1, ['n']],
                    operation = datafact1_1.operation
                )
                datafact1_2 = Datafact(
                    subject = [parents1_2, column_name1_2, ['n-1']],
                    operation = datafact1_2.operation
                )
                datafacts2 = Datafact(
                    subject = [parents, column_name, ['*']],
                    operation = [
                        'Rank', 
                        order, 
                        Datafact(
                            subject=[parents, column_name, ['*']],
                            operation=[operation_name1, op, datafact1_1, datafact1_2]
                        )
                    ]
                )
                datafacts_l.append(datafacts2)
            else:
                pass
    return datafacts_l