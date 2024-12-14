from logging import basicConfig,INFO,getLogger
import os
from pathlib import Path
import inspect
from datetime import date

PROJ_DIR = Path(__file__).resolve().parent.parent
TODAY = date.today().strftime("%Y-%m-%d")
"""
ログに関する設定を行う。
【root_flg=Trueの時】
- m1.pyを実行した場合、log/m1/日付.logに保存される
- m2.pyからm1.pyを呼び出し、m2.pyを実行した場合、m1.pyのログも全てlog/m2/日付.logに保存される
【root_flg=Falseの時】
- m1.pyを実行した場合、log/m1/日付.logに保存される
- m2.pyからm1.pyを呼び出し、m2.pyを実行した場合、m1.pyのログはlog/m1/日付.logに保存され、m2.pyのログはlog/m2/日付.logに保存されるはず、、
"""
def setup_logger(root_flg=True):
    # 呼び出し元スクリプトのファイル名を取得
    stack = inspect.stack()
    root_filename = stack[-1].filename
    script_filename = stack[0].filename

    name = root_filename if(root_flg) else script_filename
    name = name.split("/")[-1]

    # ログディレクトリを作成
    os.makedirs(PROJ_DIR/f'log/{name}', exist_ok=True)

    # ロガーを設定
    basicConfig(
        filename=PROJ_DIR/f'log/{name}/{TODAY}.log',
        filemode="a",
        format="%(asctime)s - %(name)s\n%(message)s",
        level=INFO
    )

    # スクリプト名をロガー名に設定
    logger = getLogger(name)
    return logger