from pathlib import Path
import pandas as pd
import time
from datetime import datetime
# 自分で定義した関数・クラスの読み込み
from determine_attribute_type import determine_attribute_type
from define_drilldown_path import define_drilldown_path
from make_operation import define_aggregation_F, define_scalar_arithmetic_operator
from logging_config import setup_logger


PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/"data"
PROMPT_DIR = DATA_DIR/"prompt"

logger = setup_logger()


"""
以下のようなdf周りのメタデータを保存するクラス
- df: データフレーム
- sample_df: 属性タイプの判定などに使用するGPTに投げるようデータ
- df_description: dfの説明文
- analysis_goal: 分析の目的
- focus_attr_l: 計算対象とする属性リスト
- attr_type_d: 属性のタイプ(Categoricalとか)情報の辞書
- aggregation_attr_l: 計算を実施する属性を格納したリスト
- aggregation_f_d: 被計算属性ごとにaggregation関数を保存した辞書
- operator_d: (被計算属性,aggregation_f)ごとにScalarArithmetic用演算子を格納した辞書
"""
class DataFrameMetaInfo:
    def __init__(self, df, sample_df, df_description, analysis_goal, focus_attr_l):
        self.df = df
        self.sample_df = sample_df
        self.df_description = df_description
        self.analysis_goal = analysis_goal
        self.focus_attr_l = focus_attr_l  

        print(f"\n{datetime.fromtimestamp(time.time())}::各属性タイプの判定を開始")
        self.attr_type_d = determine_attribute_type(sample_df=sample_df)
        print(f"\n{datetime.fromtimestamp(time.time())}::ドリルダウンの定義を開始")
        self.drilldown_path_l = define_drilldown_path(
            main_df=self.df, 
            sample_df=self.sample_df, 
            description=self.df_description, 
            analysis_goal=self.analysis_goal, 
            attribute_type=self.attr_type_d
        )
        print(f"\n{datetime.fromtimestamp(time.time())}::Aggregation関数の選択を開始")
        self.aggregation_f_d = define_aggregation_F(
            att_l = self.focus_attr_l, 
            att_label_d = self.attr_type_d
        )
        print(f"\n{datetime.fromtimestamp(time.time())}::ScalarArithmeticの演算子選択を開始")
        self.operator_d = define_scalar_arithmetic_operator(
            focus_att_l = self.focus_attr_l, 
            focus_att2func_d = self.aggregation_f_d, 
            attr_type_d = self.attr_type_d
        )


if __name__ == '__main__':
    main_df = pd.read_csv(DATA_DIR/'tables/_amazon-purchases.csv')
    sample_df = main_df.head(2)
    description = "2018年から2022年にかけての米国の5027人のAmazon.comユーザーの購入履歴。データセットのサイズは300MB超。"
    analysis_goal = "地理的な購買パターンの分析"
    focus_attr_l = ["Purchase Price Per Unit","Quantity"]
    df_meta_info = DataFrameMetaInfo(
        df=main_df, 
        sample_df=sample_df, 
        df_description=description, 
        analysis_goal=analysis_goal,
        focus_attr_l=focus_attr_l
    )