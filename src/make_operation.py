

"""
入力: (att_l, att_label_d)=(["売上","従業員数","上場flg"],{"売上":"Quantitative","上場flg":"Categorical",...})
出力: {"売上":[1,3,5],"従業員数":[1,3,5],"上場flg":[2]}
"""
def make_operation(att_l, att_label_d):
    result_d = {}
    operation_d = {
        1:"合計値を計算(sum)",
        2:"平均値を計算(mean)",
        3:"最大値を計算(max)",
        4:"最小値を計算(min)",
        5:"中央値を計算(meadian)",
        6:"データ数をカウント(count)",
        7:"ユニークな値の数をカウント(nunique)",
        8:"ユニークな値を列挙(unique)"
        }
    for att_name in att_l:
        instruction = f"「{att_name}」属性に適応したい操作を以下から選択し、その数字をカンマ(,)区切りで入力してください"
        if(att_label_d[att_name]=="Quantitative"):
            choices = "\n".join([f"{k}:{v}" for k, v in operation_d.items() if(k<=6)])
            user_choice = [int(n) for n in input(instruction+"\n\n"+choices+"\n").split(",")]
        else:
            choices = "\n".join([f"{k}:{v}" for k, v in operation_d.items() if(k>=6)])
            user_choice = [int(n) for n in input(instruction+"\n\n"+choices+"\n").split(",")]
        result_d[att_name] = user_choice
    return result_d
