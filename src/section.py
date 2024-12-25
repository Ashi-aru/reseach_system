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

# ロガーの設定
logger = setup_logger()

load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")
with open(DATA_DIR/"prompt/sentences2section.txt", "r") as f:  
    BASE_PROMPT = f.read()
MODEL = "o1-mini"


"""
(被Agg属性,Aggregation_f)の組み合わせごとに生成するSeciton(節)を定義するクラス
【インスタンス】
- attr_and_f_tuple: この節で扱う(被Agg属性,Aggregation_f)の組み合わせ（タプル）
- sentences: 生成した文章を格納する辞書。
    - キーは0,1,2,..(何文目か)
    - 値は文章
- sentenceNum_to_datafactIndex_d: 各文の元になったdatafactを格納する辞書
    - キーは0,1,2,..(何文目か)
    - 値は元となったdatafactのインデックス
- sentenceNum_to_datafactsResult_d: 各文の元になったdatafactについて、それが含まれるdatafactsデータたちを格納する辞書
    - キーは0,1,2..(何文目か)
    - 値は元となったdatafactが含まれるdatafactsデータ群を格納したリスト
- based_datafact_l: 本節で使用するdatafactを格納するリスト
【メソッド】
- make_section: 本節での文章生成を行うメソッド
    - drilldown: 指定したdatafact.subjectから1-p値を元にドリルダウンしてdatafactを列挙
    - make_templates: 列挙したdatafactをテンプレート化
    - render_sentence: テンプレート化したdatafactに値をレンダリング
    - make_section_sentences: 各文からレポートを生成
    - レポートからデータが足りないdatafact.subjectを列挙
"""
class Section:
    def __init__(self, attr_and_f_tuple):
        self.attr_and_f_tuple = attr_and_f_tuple
        self.sentenceNum_to_sentence_d = {}
        self.sentenceNum_to_datafactIndex_d = {}
        self.sentenceNum_to_datafactsResult_d = {}
        self.based_datafact_l = []

    
    def make_section(self, datafact_l, manager, ordinal_d, df_meta_info):
        self.based_datafact_l = datafact_l
        table_description = df_meta_info.df_description
        """
        テンプレート化したdatafactに値をレンダリングする関数
        """
        def render_sentence(datafact):
            template = Template(manager.search_template(datafact.subject, datafact.operation))
            value = manager.search_result(datafact.subject, datafact.operation)
            if(value is None):
                return None
            return template.render(value=value)
        """
        各文からsectionの文章を生成する関数
        【入力】
        - sentences_l: 各文が格納されたリスト（インデックスがbased_datafact_lのインデックスに一致）
        【出力】
        - section: OpenAi APIの出力のcontent部分
        - message: OpenAi APIに入力した部分
        """
        def make_section_sentences(sentences_l, df_meata_info, model, previous_message=None):
            print(f"\n{datetime.fromtimestamp(time.time())}::節の文章生成を開始\nmodel = {model}")
            drilldown_path = "=>".join(df_meata_info.drilldown_path_l)
            sentences_d = dict([(i,s)for i, s in enumerate(sentences_l)])
            data = {
                "drilldown_path":drilldown_path,
                "agg_attr":self.attr_and_f_tuple[0],
                "agg_f":self.attr_and_f_tuple[1],
                "table_description":df_meata_info.df_description
            }
            client = OpenAI(api_key=API_KEY)
            prompt = Template(BASE_PROMPT).render(data)
            messages = [
                    {"role": "user", "content": prompt},
                    {"role": "user", "content": "入力:\n"+json.dumps(sentences_d,ensure_ascii=False,indent=4)+"\n\n出力:\n"},
                    ]
            if(previous_message):
                messages = previous_message + messages
            response = client.chat.completions.create(model=model, messages=messages)
            # logger.info(f'section.py\n{response.choices[0].message.content}')
            content = json.loads(response.choices[0].message.content)
            response = to_dict_recursive(response)
            logger.info(f'section.py\n{content}')
            print(f"{datetime.fromtimestamp(time.time())}::節の文章生成を終了")
            return [content, messages]
        
        make_templates(datafact_l, manager, ordinal_d, table_description, model="o1-preview") 
        self.based_datafact_text_l = [render_sentence(datafact) for datafact in datafact_l]
        content, messages = make_section_sentences(self.based_datafact_text_l, df_meta_info, model='gpt-4o')
        # ambiguous_datafact = [datafact_l[i] for i in content["ambiguous_datafact"]]
        # if(ambiguous_datafact!=[]):
        #     make_templates(ambiguous_datafact, manager, ordinal_d, table_description, model="o1-preview")
        #     sentences_l = [render_sentence(datafact) for datafact in datafact_l]
        #     for s in sentences_l:
        #         print(s)
        #     messages.append({"role": "user", "content": "解釈が一意でないデータファクトについて、修正しました。ambiguous_datafactとしたデータファクトを再確認してもらい、もう一度同じタスクをお願いします。"})
        #     content, messages = make_section_sentences(sentences_l, df_meta_info, model='gpt-4o', previous_message=messages)

        all_senteces = ''
        for k, v in content['sentences'].items():
            self.sentenceNum_to_sentence_d[k] = v['sentence']
            self.sentenceNum_to_datafactIndex_d[k] = v['based_datafact']
            all_senteces += v['sentence']
        logger.info(f"Section.make_section\n{all_senteces}")

        for k, v in content['sentences'].items():
            for datafact_n in v['based_datafact']:
                for datafacts in datafact2datafacts(datafact_l[datafact_n], manager=manager):
                    results = manager.search_result(datafacts.subject, datafacts.operation)
                    if(k in self.sentenceNum_to_datafactsResult_d):
                        self.sentenceNum_to_datafactsResult_d[k].append(results)
                    else:
                        self.sentenceNum_to_datafactsResult_d[k] = [results] 
        return None




        