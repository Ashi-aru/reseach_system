from pathlib import Path
import logging
from datetime import date


PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/"data"
TODAY = date.today().strftime("%Y-%m-%d")

logging.basicConfig(
        level=logging.DEBUG,
        filename=PROJ_DIR/f'log/others/{TODAY}.log',
        format='%(asctime)s\n%(message)s'
    )

# responseはChatCompletionオブジェクトで、これをリストに格納してしまうと、jsonとして保存ができないので、
# ChatCompletionオブジェクトを再帰的に計算し、ネストした辞書に変換する関数を定義
def to_dict_recursive(chat_completion_object):
    d = {
        "id": chat_completion_object.id,
        "object": chat_completion_object.object,
        "created": chat_completion_object.created,
        "model": chat_completion_object.model,
        "system_fingerprint": chat_completion_object.system_fingerprint,
        "choices": [
            {
                "index": chat_completion_object.choices[0].index,
                "message":{
                    "role": chat_completion_object.choices[0].message.role,
                    "content": chat_completion_object.choices[0].message.content,
                },
                "logprobs":chat_completion_object.choices[0].logprobs,
                "finish_reason":chat_completion_object.choices[0].finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": chat_completion_object.usage.prompt_tokens,
            "completion_tokens": chat_completion_object.usage.completion_tokens,
            "total_tokens": chat_completion_object.usage.total_tokens,
            # "completion_tokens_details": {
            #     "reasoning_tokens": chat_completion_object.usage.completion_tokens_details.reasoning_tokens,
            # }
        },
        "time": TODAY
    }
    return d


# parents(Item Filteringする条件)とdfが与えられたとき、parentsに沿って抽出したdfを出力
# 入力: (parents, df)→({"県":"東京都", "年":2022}, df)
# 出力: new_df
def filter_df_by_parents(parents, df):
    if(len(parents)==0):
        return df
    condition = (df[list(parents.keys())[0]]==list(parents.values())[0])
    for k, v in parents.items():
        condition &= (df[k]==v)
    return df[condition]

"""
datafact.subjectが与えられた時に、datafact or datafactsを判定。
datafactsの場合は、何の集合か（年?県?）を示すAttribute名も返す
入力: datafact.subject
出力: [True/False, Attribute名]
"""
def is_datafacts(subject):
    parents, col_name, filter_values = subject
    is_datafacts = (
            filter_values==["*"] or ("*" in parents.values())
            or
            filter_values==['n'] or ('n' in parents.values())
            or
            filter_values==['n-1'] or ('n-1' in parents.values())
        )
    key_attr = None
    if(filter_values==["*"]): 
        key_attr = col_name
    else:
        for k, v in parents.items():
            if(v=="*"):
                key_attr = k
    return [is_datafacts, key_attr]