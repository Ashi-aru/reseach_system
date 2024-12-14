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
        filename=PROJ_DIR/f'log/determine_attribute_type/{TODAY}.log',
        format='%(asctime)s\n%(message)s'
    )


# df(もしくはサンプルデータのdf)を受け取り、メタデータを生成する
def make_metadata(df):
    df = df.head(2)
    return_d = dict([(column_name, list(df[column_name])) for column_name in df.columns])
    return json.dumps(return_d)

# 表データに関するメタデータから各属性のタイプ（Categorical, Ordinal, Ordinal_t, Quantitative）を推論する
def reasoning_about_attribute_type(metadata):
    API_KEY = os.getenv('OPENAI_API_KEY')
    MODEL = "gpt-4o-2024-08-06" # gpt-4o-mini, gpt-4o-2024-05-13, gpt-4o-2024-08-06
    with open(PROMPT_DIR/"determine_attribute_type.txt", "r") as f:  
        BASE_INSTRUCTION = f.read()

    client = OpenAI(api_key=API_KEY)
    messages = [
            {"role": "system", "content": BASE_INSTRUCTION},
            {"role": "user", "content": "Input:\n"+metadata+"\n\nOutput:\n"},
            ]
    response = client.chat.completions.create(model=MODEL, messages=messages)
    content = response.choices[0].message.content
    response = to_dict_recursive(response)
    logging.info(content)
    return [content, response]


def determine_attribute_type(sample_df):
    metadata = make_metadata(sample_df)
    label_result = reasoning_about_attribute_type(metadata)
    return label_result

if __name__ == "__main__":
    determine_attribute_type(DATA_DIR/'amazon-purchases.csv')