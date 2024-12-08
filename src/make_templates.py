from dotenv import load_dotenv
import os
from openai import OpenAI
from datetime import date
import json
from pathlib import Path
import logging
import re
# 自分で定義した関数、クラスのインポート
from datafact_manager import DatafactManager
from datafact_model import Datafact
from others import to_dict_recursive


PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/'data'
TODAY = date.today().strftime("%Y-%m-%d")

logging.basicConfig(
    level=logging.INFO,
    filename=PROJ_DIR/f'log/make_templates/{TODAY}.log',
    format='%(asctime)s\n%(message)s'
)

load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")
with open(DATA_DIR/"prompt/make_templates.txt", "r") as f:  
        BASE_PROMPT = f.read()
MODEL = "gpt-4o-2024-05-13"



"""
プロンプトを作成する関数
td_flgは表データの説明文についての置き換えを実施するかどうか
dn_flgはdatafact_numの置き換えを実施するかどうか
入力:(表データの説明文, datafactの数, td_flg, dn_flg) = (str, end-start)
出力:プロンプト(string)
"""
def make_prompt(base_prompt, table_description=None, datafact_num=None, td_flg=False, dn_flg=False):
    if(td_flg):
        base_prompt = re.sub(r"\[\[table_description\]\]", table_description, base_prompt, count=1)
    if(dn_flg):
        base_prompt = re.sub(r"\[\[datafact_num\]\]", str(datafact_num), base_prompt, count=2)
    return base_prompt

"""
テンプレートを出力する関数
入力: datafactのリスト
出力: None(各datafactに言及するテンプレートをDatafactManager.templatesに保存)
"""
def make_templates(datafact_l, manager, ordinal_d, table_description):
    start, end, step_n = 0, 0, 10
    base_prompt = make_prompt(BASE_PROMPT, table_description=table_description, td_flg=True)
    while end < len(datafact_l):
        flows_d = {}
        # テンプレート生成をstep_n個のdatafactごとに実施（APIに送信）
        end = start+step_n if(start+step_n<=len(datafact_l)) else len(datafact_l)
        for i, datafact in enumerate(datafact_l[start:end]):
            flows_d[f"flow_{start+i}"] = datafact.convert_datafact_to_operationflow(ordinal_d)
        # logging.info(json.dumps(flows_d,ensure_ascii=False,indent=4))

        client = OpenAI(api_key=API_KEY)
        prompt = make_prompt(base_prompt,datafact_num=end-start,dn_flg=True)
        messages = [
                {"role": "system", "content": prompt},
                {"role": "user", "content": "Input:\n"+json.dumps(flows_d,ensure_ascii=False,indent=4)+"\n\nOutput:\n"},
                ]
        response = client.chat.completions.create(model=MODEL, messages=messages)
        content = json.loads(response.choices[0].message.content)
        response = to_dict_recursive(response)
        logging.info(json.dumps(content,ensure_ascii=False,indent=4))

        for i,(k,v) in enumerate(content.items()):
            # logging.info(f"=====================\n{k}\n{v}")
            datafact, template = datafact_l[start+i], v["template"]
            manager.update_templates(datafact.subject, datafact.operation, template)
        start = end
    return None