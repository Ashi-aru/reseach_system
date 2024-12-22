import pandas as pd
from pathlib import Path
from datetime import date
# 自分で定義した関数・クラスをimport
from logging_config import setup_logger

PROJ_DIR = Path(__file__).resolve().parent.parent
TODAY = date.today().strftime("%Y-%m-%d")

logger = setup_logger()


class Report:
    def __init__(self):
        self.report = {}
        

    