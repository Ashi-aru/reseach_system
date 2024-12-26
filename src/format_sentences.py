from dotenv import load_dotenv
import os
from openai import OpenAI
import json
from pathlib import Path
import time
from datetime import datetime, date
from jinja2 import Template
# 自分で作った関数の読み込み
from logging_config import setup_logger
from debug import debug_datafact
from make_templates import make_templates
from others import to_dict_recursive
from make_datafact import datafact2datafacts


PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/"data"
PROMPT_DIR = DATA_DIR/"prompt"

TODAY = date.today().strftime("%Y-%m-%d")
API_KEY = os.getenv("OPENAI_API_KEY")
with open(DATA_DIR/"prompt/format_sentences.txt", "r") as f:  
    BASE_PROMPT = f.read()

# ロガーの設定
logger = setup_logger()

"""
節、章、レポートについて、文章を編集し、より読みやすくするための関数
【入力】
- sentences: section, chapter, reportオブジェクト
【出力】
- sentences: section, chapter, reportオブジェクト
    - object.sentenceNum_to_sentence_d, object.sentenceNum_to_datafactIndex_dを修正したものを出力
"""
def format_sentences(s_obj, papaer_name, df_meta_info, model='gpt-4o'):
    print(f"\n{datetime.fromtimestamp(time.time())}::文章の編集を開始\nmodel = {model}")
    sentences_num = len(s_obj.sentenceNum_to_sentence_d)
    draft = dict([
        (
            str(i), 
            {"sentence":s_obj.sentenceNum_to_sentence_d[str(i)], "based_datafact":s_obj.sentenceNum_to_datafactIndex_d[str(i)]}
        )
        for i in range(sentences_num)
    ])
    input_d = {"draft":draft, "based_datafact":s_obj.based_datafact_text_l}

    drilldown_path = "=>".join(df_meta_info.drilldown_path_l)
    data = {
        "drilldown_path":drilldown_path,
        "paper_name":papaer_name,
        "table_description":df_meta_info.df_description,
    }
    client = OpenAI(api_key=API_KEY)
    prompt = Template(BASE_PROMPT).render(data)
    messages = [
            {"role": "user", "content": prompt},
            {"role": "user", "content": "#入力:\n"+json.dumps(input_d,ensure_ascii=False,indent=4)+"\n\n#出力:\n"},
            ]

    response = client.chat.completions.create(model=model, messages=messages)
    content = json.loads(response.choices[0].message.content)
    response = to_dict_recursive(response)
    logger.info(f'format.py\n{content}')

    n_sentenceNum_to_sentence_d = {}
    n_sentenceNum_to_datafactIndex_d = {}
    all_senteces = ''
    for k, d in content.items():
        n_sentenceNum_to_sentence_d[k] = d["sentence"]
        n_sentenceNum_to_datafactIndex_d[k] = d["based_datafact"]
        all_senteces += d['sentence']
    logger.info(f"Section.make_section\n{all_senteces}")
    s_obj.sentenceNum_to_sentence_d = n_sentenceNum_to_sentence_d
    s_obj.sentenceNum_to_datafactIndex_d = n_sentenceNum_to_datafactIndex_d
    print(f"{datetime.fromtimestamp(time.time())}::文章の編集を終了")
    return [content, messages]