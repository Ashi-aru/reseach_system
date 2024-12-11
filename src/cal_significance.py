import logging
from pathlib import Path
import math
from datetime import date
from scipy.stats import t
import numpy as np
# 自分が定義したクラス、関数をインポート


PROJ_DIR = Path(__file__).resolve().parent.parent
TODAY = date.today().strftime("%Y-%m-%d")

logging.basicConfig(
        level=logging.INFO,
        filename=PROJ_DIR/f'log/cal_significance/{TODAY}.log',
        format='%(asctime)s\n%(message)s'
    )

"""
Grubbs検定で外れ値を検出し、そのp値を計算する関数。
以下の2ステップを繰り返す（[外れ値が検出されなくなる or valuesの長さが3未満になる]まで）
1. 最大値方向の外れ値検出→最大値を除去
2. 最小値方向の外れ値検出→最小値を除去


入力
- data: 数値のリスト
    - {"製造業":100, "サービス業":500,...,"その他":150}
- alpha:有意水準
出力
- max方向の外れ値のリスト ["サービス業",..]
- min方向の外れ値のリスト ["製造業",...]
"""
def detect_outliers_with_p_values_both_directions(data, alpha=0.05):
    """
    outliersライブラリを用いて複数の外れ値を最大値方向と最小値方向に対して検出し、
    p値を計算して出力する。
    """
    result_d = {}
    while len(data) >= 3:  # Grubbs検定には少なくとも3つのデータが必要
        values, keys = list(data.values()), list(data.keys())
        n = len(values)
        mean, std = np.mean(values), np.std(values)

        abs_diff = np.abs(values - mean)
        max_diff_index = np.argmax(abs_diff)
        suspect_value, suspect_name = values[max_diff_index], keys[max_diff_index]

        G = abs(suspect_value - mean) / std
        t_value = math.sqrt(n*(n-2)*(G**2) / ((n-1)**2 - n*(G**2))) # ((n-1)**2 * G**2) / (n - (n-1)*
        p_value = (1 - t.cdf(t_value, n-2))*(2*n)
        
        if(p_value <= alpha):
            del data[suspect_name]
            result_d[suspect_name] = {"p_value": p_value, "data_value": suspect_value}
        else:
            break  
    return result_d

# 使用例
values = dict([(str(i),n) for i, n in enumerate([10, 12, 15, 18, 100, 102, 120, -20])])
alpha = 0.05


