from pathlib import Path
import os
from openai import OpenAI
import logging
from datetime import date
import pandas as pd
import json
# 自分で作った関数の読み込み
from others import to_dict_recursive

PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/"data"
PROMPT_DIR = DATA_DIR/"prompt"

TODAY = date.today().strftime("%Y-%m-%d")

logging.basicConfig(
        level=logging.DEBUG,
        filename=PROJ_DIR/f'log/define_drilldown_path/{TODAY}.log',
        format='%(asctime)s\n%(message)s'
    )


# ----------------------------------------------------------------
# ① drilldown案の列挙

# df(もしくはサンプルdf)とattributetypeの辞書から、Ordinal,Categorical属性に関するプロンプト用サンプルデータを生成
def make_sample_data(sample_df, attribute_type):
    sample_df = sample_df.head(2)
    sample_data = {}
    for att_name, att_type in attribute_type.items():
        sample_data[att_name] = {
            "values":list(sample_df[att_name]),
            "attribute_type":att_type
        }
    return sample_data

# ドリルダウンの案を列挙する
# 入力：{"description":表データの説明, "analysis_goal":データ分析の目標, "sample_data":サンプルデータ}
# サンプルデータは上のmake_sample_dataで生成する
def list_drilldown_ideas(input_data):
    API_KEY = os.getenv('OPENAI_API_KEY')
    MODEL = "o1-preview" # gpt-4o-mini, gpt-4o-2024-05-13, gpt-4o-2024-08-06, o1-preview
    with open(PROMPT_DIR/"define_drilldown_path.txt", "r") as f:  
        BASE_INSTRUCTION = f.read()

    client = OpenAI(api_key=API_KEY)
    messages = [
            {"role": "user", "content": BASE_INSTRUCTION},
            {"role": "user", "content": "Input:\n"+input_data+"\n\nOutput:\n"},
            ]
    response = client.chat.completions.create(model=MODEL, messages=messages)
    content = response.choices[0].message.content
    response = to_dict_recursive(response)
    logging.info(content)
    return [content, response]


# ----------------------------------------------------------------
# ② 列挙したドリルダウン案の中から、ノード数などの条件に反するものを排除する

# dfとattribute_typeを受け取り、各Categorical, Ordinal属性のユニークな値の数を辞書にまとめる
# attribute_typeはdetermine_attribute_typeの出力（1つ目）を、dictに変換したものを使用
def count_unique_data(main_df, attribute_type):
    unique_num_d = {}
    for att_name, att_type in attribute_type.items():
        if(att_type == "Ordinal" or att_type == "Categorical"):
            unique_num_d[att_name] = main_df[att_name].nunique(dropna=False)
    return unique_num_d

# ①で生成したdrilldown案が制限に反していないか判定→フラグをつける（辞書の更新）
def check_drilldown_ideas(drilldown_ideas, main_df, attribute_type):
    unique_num_d = count_unique_data(main_df, attribute_type)
    for idea_n, drilldown in drilldown_ideas.items():
        node_n = 1
        for column_name in drilldown["drilldown"]:
            node_n *= unique_num_d[column_name]
        drilldown_ideas[idea_n]["flg"] = (node_n<10**6)
    logging.info(drilldown_ideas)
    return drilldown_ideas


# ----------------------------------------------------------------
# ③ ①,②のまとめ

def define_drilldown_path(main_df, sample_df, description, analysis_goal, attribute_type):
    # ①
    sample_data = make_sample_data(sample_df=sample_df, attribute_type=attribute_type)
    input_data = json.dumps({"description":description, "analysis_goal":analysis_goal, "sample_data":sample_data})
    output = list_drilldown_ideas(input_data=input_data)
    # ②
    drilldown_ideas = json.loads(output[0])
    drilldown_ideas = check_drilldown_ideas(drilldown_ideas=drilldown_ideas, main_df=main_df, attribute_type=attribute_type)
    return drilldown_ideas


if __name__ == "__main__":
    main_df = pd.read_csv(DATA_DIR/'amazon-purchases.csv')
    sample_df = main_df.head(2)
    description = "2018年から2022年にかけての米国の5027人のAmazon.comユーザーの購入履歴。データセットのサイズは300MB超。"
    analysis_goal = "地理的な購買パターンの分析"
    attribute_type = {
        "Order Date": "Ordinal",
        "Purchase Price Per Unit": "Quantitative",
        "Quantity": "Quantitative",
        "Shipping Address State": "Categorical",
        "Title": "Categorical",
        "ASIN/ISBN (Product Code)": "Categorical",
        "Category": "Categorical",
        "Survey ResponseID": "Categorical"
    }

    ideas = define_drilldown_path(main_df=main_df,sample_df=sample_df,description=description,analysis_goal=analysis_goal,attribute_type=attribute_type)
