from pathlib import Path
from datetime import date
# 自分で定義した関数・クラスをimport
# from datafact_model import Datafact
from logging_config import setup_logger

PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/"data"
TODAY = date.today().strftime("%Y-%m-%d")

logger = setup_logger()


"""
データファクトをパーツごとに分解し、文字列としてリストに格納する関数
入力
- datafact: Datafactオブジェクト
出力
- datafactを文字列にしたもの（\t,\nを含む形で）
"""
def unpack_datafact(datafact):
    part_l = []
    parents, column_name, filter_values = datafact.subject
    operation_name, *operation_others = datafact.operation
    subject_str = f"[{parents},{column_name},{filter_values}]"
    if(operation_name=="Aggregation"):
        aggregation_col, f_name = operation_others
        part_l.append(f"Datafact(subject={subject_str},operation=[Aggregation,{aggregation_col},{f_name}]),")
        return part_l
    elif(operation_name=="ScalarArithmetic"):
        op, datafact1, datafact2 = operation_others
        l = ["Datafact(", f"subject={subject_str},", "operation=[", "ScalarArithmetic,", f"{op},"]+unpack_datafact(datafact1)+unpack_datafact(datafact2)+["]", ")"]
        for part in l:
            part_l.append(part)
        return part_l
    elif(operation_name=="Rank"):
        order, datafacts = operation_others
        l = ["Datafact(", f"subject={subject_str},", "operation=[", "Rank,", f"{order},"]+unpack_datafact(datafacts)+["]", ")"]
        for part in l:
            part_l.append(part)
        return part_l
    else:
        raise ValueError("Unknown operation")

"""
operationをパーツごとに分解し、文字列としてリストに格納する関数
入力
- operation: Datafact.operation
出力
- operationを文字列にしたもの（\t,\nを含む形で）
"""
def unpack_operation(operation):
        part_l = []
        operation_name, *operation_others = operation
        if(operation_name=="Aggregation"):
            aggregation_col, f_name = operation_others
            part_l.append(f",operation=[{operation_name},{aggregation_col},{f_name}],")
            return part_l
        elif(operation_name=="ScalarArithmetic"):
            op, datafact1, datafact2 = operation_others
            l = ["operation=[", "ScalarArithmetic,", f"{op},"]+unpack_datafact(datafact1)+unpack_datafact(datafact2)+["]"]
            for part in l:
                part_l.append(part)
            return part_l
        elif(operation_name=="Rank"):
            order, datafacts = operation_others
            l = ["operation=[", "Rank,", f"{order},"]+unpack_datafact(datafacts)+["]"]
            for part in l:
                part_l.append(part)
            return part_l
        else:
            raise ValueError("Unknown operation")

"""
datafactをデバックする関数。
普通にデバックすると、['ScalarArithmetic', '-', <datafact_model.Datafact object at 0x10615fac0>, <datafact_model.Datafact object at 0x10615f280>]となってしまい、中身が確認できない。
→ 中身を確認できるようにする
入力:
- datafact
出力
- string
"""
def debug_datafact(datafact):
    part_l = unpack_datafact(datafact)
    all_str, tab_n = "", 0
    for part in part_l:
        if(part in [")", "]"]):
            tab_n -= 1
        all_str += ("    "*tab_n + part + "\n")
        if(part in ["Datafact(","operation=["]):
            tab_n += 1
    return all_str

"""
operationをデバックする関数。
普通にデバックすると、['ScalarArithmetic', '-', <datafact_model.Datafact object at 0x10615fac0>, <datafact_model.Datafact object at 0x10615f280>]となってしまい、中身が確認できない。
→ 中身を確認できるようにする
入力:
- datafact.operation
出力
- string
"""
def debug_operation(operation):
    part_l = unpack_operation(operation)
    all_str, tab_n = "", 0
    for part in part_l:
        if(part in [")", "]"]):
            tab_n -= 1
        all_str += ("    "*tab_n + part + "\n")
        if(part in ["Datafact(","operation=["]):
            tab_n += 1
    return all_str

"""
if __name__ == "__main__":
    datafact = Datafact(
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
    logger.info(debug_datafact(datafact))
    logger.info(debug_operation(datafact.operation))
"""