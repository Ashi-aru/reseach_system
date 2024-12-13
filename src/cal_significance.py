import logging
from pathlib import Path
import math
from datetime import date
from scipy.stats import t
import scipy.stats as stats
import numpy as np
import random
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
以下の3ステップを繰り返す（[外れ値が検出されなくなる or valuesの長さが3未満になる]まで）
1. 平均から最も外れた値を選出
2. 外れ値の判定を行う
3. 外れ値の場合、p値を保存→この値をデータ群から除外

入力
- data: 数値のリスト
    - {"製造業":100, "サービス業":500,...,"その他":150}
- alpha:有意水準
出力
- 外れ値の辞書
    - {"サービス業":{"p_value":0.002, "data_value":150},..}
"""
def detect_outliers(data, alpha=0.05):
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
        if((n-1)**2 - n*(G**2)>0):
            t_value = math.sqrt(n*(n-2)*(G**2) / ((n-1)**2 - n*(G**2)))
            p_value = (1 - t.cdf(t_value, n-2))*(2*n)
        else:
            """
            t_valueのルートの中身が負になる場合はp値を0にする。
            G > (n-1)/sqrt(n)の時に、ルートの中身が0になる。
            G = abs(x-mean)/stdより、abs(x-mean)が大きすぎるとルートの中身が0になる
            → めちゃくちゃ外れ値ということなので、p値は0でいいのではないでしょうか？
            """
            p_value = 0  # 計算できない場合は0を代入
        
        if(p_value <= alpha):
            del data[suspect_name]
            result_d[suspect_name] = {"p_value": p_value, "data_value": suspect_value}
        else:
            break  
    return result_d


if(__name__=="__main__"):
    def smirnov_grubbs(data, alpha):
        x, o = list(data), []
        while True:
            n = len(x)
            t = stats.t.isf(q=(alpha / n) / 2, df=n - 2)
            tau = (n - 1) * t / np.sqrt(n * (n - 2) + n * t * t)
            i_min, i_max = np.argmin(x), np.argmax(x)
            myu, std = np.mean(x), np.std(x, ddof=1)
            i_far = i_max if np.abs(x[i_max] - myu) > np.abs(x[i_min] - myu) else i_min
            tau_far = np.abs((x[i_far] - myu) / std)
            if tau_far < tau: break
            o.append(x.pop(i_far))
        return o #(np.array(x), np.array(o))


    # データの基本設定
    normal_data = [random.uniform(0, 500) for _ in range(1000)]  # 正常値
    outliers = [random.uniform(800, 1000), random.uniform(-10, 0)]  # 外れ値
    # 正常値と外れ値を混ぜる
    data = normal_data + outliers
    random.shuffle(data) 
    # 実行
    alpha = 0.05
    print(smirnov_grubbs(data, alpha))
    print(detect_outliers(data, alpha))