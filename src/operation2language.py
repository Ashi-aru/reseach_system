# データ操作→言語化
from pathlib import Path
from datetime import date
# 自分で定義した関数・クラスをimport
from logging_config import setup_logger

PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR/"data"
TODAY = date.today().strftime("%Y-%m-%d")

logger = setup_logger()

# 問い合わせ形式：['Attribute名', '関数名']
# 入力例：['売上', 'sum']
# 出力例：'売上の合計値'
def Aggregation(request):
    attribute, func = request
    d_func = {'sum':'合計値','sum_percent':'合計値の割合','mean':'平均値','max':'最大値','min':'最小値','median':'中央値','count':'データ数','count_percent':'データ数の割合','nunique':'ユニークな値の数','unique':'ユニークな値'}
    # d_func = {'mean':'平均値','count':'データ数','sum':'合計値','min':'最小値','max':'最大値','median':'中央値','nunique':'ユニークな値の数','unique':'ユニークな値'}
    T = d_func[func] if(attribute=="") else f'{attribute}の{d_func[func]}'
    return T+"を計算"

# 問い合わせ形式：['演算子', [Attributes名のリスト]]
# 入力例：['+', ['A1',['*', ['A2','A3']]]]
# 出力例：'「A1と「A2とA3の積」の和」'
def AttributeArithmetic(request):
    def is_atom(term):
        return isinstance(term, str)
    d_operator = {'+':'和', '-':'差', '*':'積', '/':'商'}
    operator, terms_l = request
    if(terms_l[0]=='' and terms_l[1]==''):
        return f'「両者の{d_operator[operator]}」'
    elif(terms_l[0]==''):
        return f'前者と{terms_l[1]}の{d_operator[operator]}'
    elif(terms_l[1]==''):
        return f'{terms_l[0]}と後者の{d_operator[operator]}'
    else:
        T1 = terms_l[0] if(is_atom(terms_l[0])) else AttributeArithmetic(terms_l[0])
        T2 = terms_l[1] if(is_atom(terms_l[1])) else AttributeArithmetic(terms_l[1])
        return f'「{T1}と{T2}の{d_operator[operator]}」'      

# 問い合わせ形式：['演算子', 'Attribute名', 'value']
# 入力例：['+', '売上', '1']
# 出力例：'「売上に1を足したもの」'
def AttributeScalarArithmetic(request):
    operator = request[0]
    operator_d = {'+':'足した', '-':'引いた', '*':'掛けた', '/':'割った'}
    return f'「{request[1]}に{request[2]}を{operator_d[operator]}もの」'

# 問い合わせ形式：['in', [Attributes]]
# 入力例：['in', ['売上']]
# 出力例：'「dfから、「売上」のAttributesを抜き出したもの」'
def AttributeSelection(request):
    if(request[0]=='in'):
        return '「'+','.join(request[1])+'のAttribute」'
    else:
        return '「'+','.join(request[1])+'以外のAttribute」'
    
# 問い合わせ形式：[set1, set2]
def Difference(request):
    set1, set2 = request
    return f'「{set1}と{set2}の差集合を計算」'

# 問い合わせ形式：["Aggregate用関数名", [グループ分け用のAttributes], [集計するAttributes]]
# 入力例：['max', ['年'], ['売上','企業名']]
# 出力例：「属性"年"の値によって分けられるグループ毎に、売上の最大値とその値をとる企業名を集計したdf」
def GroupingOperation(request):
    func, groups, attributes = request
    d_func_QA = {'sum':'合計値','sum_percent':'合計値の割合','mean':'平均値','max':'最大値','min':'最小値','median':'中央値','count':'データ数','count_percent':'データ数の割合','nunique':'ユニークな値の数','unique':'ユニークな値'}
    # d_func_QA = {'mean':'平均値','count':'データ数','sum':'合計値','min':'最小値','max':'最大値','median':'中央値'}
    T1 = '属性"'+','.join(groups)+'"の値によって分けられるグループ毎に'
    t2 = f'{attributes[0]}の{d_func_QA[func]}' if(attributes[0]!="") else f'{d_func_QA[func]}'
    T2 = f'{t2}を集計したdf' if(len(attributes)==1) else f'{t2}とその値をとる{attributes[1]}を集計したdf'
    return f'「{T1}、{T2}」を作成'

# 問い合わせ形式：[set1, set2]
def Intersection(request):
    set1, set2 = request
    return f'「{set1}と{set2}の共通集合を計算」'

# 問い合わせ形式：5種類くらいある
# 入力例：['in', ['Attribute', '業種'], ['IT','食品']]
# 出力例：'「業種がIT,食品のいずれかである」'
def ItemFiltering(request):
    if(request[0]=='in'):
        if(isinstance(request[2], str)):
            return f'「{request[1][1]}が{request[2]}で計算したもののいずれかである」'
        else:
            categories = ','.join(request[2])
            text = 'のいずれかである」' if(len(request[2])>1) else 'である」'
            return f'「{request[1][1]}が'+categories+text

    if(request[0]=='<'):
        range_s = request[2] # '(2020,2024]', '[-inf,2024]'の部分を取得
        v_left, v_right = range_s[1:-1].split(',')
        s_left, s_right = '以上' if(range_s[0]=='[') else 'より大きい', '以下' if(range_s[-1]==']') else 'より小さい'
        
        if(v_left==v_right):
            s = f'{v_left}である」'
        elif(v_left=='-inf'):
            s = f'{v_right}{s_right}」'
        elif(v_right=='inf'):
            s = f'{v_left}{s_left}」'
        else:
            if(range_s[0]=='('): 
                s_left='より大きく'
            s = f'{v_left}{s_left}{v_right}{s_right}」'
        return f'「{request[1][1]}が' + s
    
    if(request[0]=='A<'):
        A1, A2 = request[1]
        operation = request[2]
        return f'「（{A1}{operation}{A2}）を満たす行」'
    
    if(request[0]=='and'):
        s1, s2 = ItemFiltering(request[1]), ItemFiltering(request[2])
        return f'『{s1}かつ{s2}』'
    if(request[0]=='or'):
        s1, s2 = ItemFiltering(request[1]), ItemFiltering(request[2])
        return f'『{s1}または{s2}』'
    if(request[0]=='not'):
        s1 = ItemFiltering(request[1])
        return f'『{s1}という条件を満たさない』'

# 問い合わせ形式：['asc/desc', 'Attribute名']
# 入力例： ['desc', '売上']
# 出力例： '売上を降順でソートし、順位のAttributeを生成'
def ItemSorting(request):
    vector, attribute = request
    d_vec = {'asc':'昇順', 'desc':'降順'}
    return f'「{attribute}を{d_vec[vector]}でソート」'

# 問い合わせ形式：['Ordered Attribute名', ['Categorical Attribute名', '注目する値']]
# 入力例： ['年', ['業界', 'IT']]
# 出力例：　'業界=ITとなる年上の区間を計算'
def Loop(request):
    OA, CA, CA_v = request[0], request[1][0], request[1][1]
    return f'「{CA}={CA_v}となる{OA}上の区間」'

# 問い合わせ形式：["key_attribute", df1, df2]
# 入力例：["年", df1, df2]
# 出力例:"df1とdf2を年で自然結合したdfを生成"
def NaturalJoin(request):
    key, df1, df2 = request
    return f'「{df1}と{df2}を{key}で自然結合したdfを生成」'

# 問い合わせ形式：['演算子', 'Value1', 'Value2']
# 入力例：['/', '今年の売上', '昨年の売上']
# 出力例：'「今年の売上を昨年の売上で割った値」'
def ScalarArithmetic(request):
    operator, v1, v2 = request
    operator_d = {'+':'和', '-':'差', '*':'積', '/':'商'}
    if(v1=='' and v2==''):
        return f'「両者の{operator_d[operator]}」'
    elif(v1==''):
        return f'「前者と{v2}の{operator_d[operator]}」'
    elif(v2==''):
        return f'「{v1}と後者の{operator_d[operator]}」'
    else:
        return f'「{request[1]}と{request[2]}の{operator_d[operator]}」'

# 問い合わせ形式:[対象データ（Attribute）, シフトする差分（+1だと下にずらす）]
def Shift(request):
    target_data, shift_diff = request
    vector = "下" if(shift_diff>0) else "上"
    return f'「{target_data}を{vector}に{shift_diff}ずらした」属性を生成'

# 問い合わせ形式：[`"attribute名"`, `昇順 or 降順`]
# 入力例：["売上", 昇順]
# 出力例：'「売上を昇順でソートしたdfを生成」'
def Sort(request):
    attribute, order = request
    # d = {0:"昇順", 1:"降順"}
    return f'「{attribute}を{order}でソート」したdfを生成'

# 問い合わせ形式：[`"対象データ名"`, `"O_attribute名"`, `ordinal_d`]
# 入力例：["売上", "年", {"年":[2021,2022,2023,2024]}]
# 出力例：'「売上を昇順でソートしたdfを生成」'
def Sort_ordinal_d(request):
    target_data, O_attribute, ordinal_d = request
    return f"{target_data}を{O_attribute}={ordinal_d[O_attribute]}の順で並び替えたdfを生成"

# 問い合わせ形式：[set1, set2]
def Union(request):
    set1, set2 = request
    return f'「{set1}と{set2}の和集合を計算」'

# 問い合わせ形式：[["インデックス名", "インデックスの値"], "抜き出すValueのattribute名"]
# 入力例：[["iloc","1"], "企業CD"]
# 出力例：'「iloc==1である行の企業CD値を取り出す」'
def ValueSelection(request):
    index_name, index_value = request[0]
    value_attribute = request[1]
    return f"「{index_name}=={index_value}であるという条件で{value_attribute}値」を抽出"


# ----------------------------------------------------
"""
subjectからItemFiltering用のリクエスト（問い合わせ）を生成する関数
入力: subject
出力: ItemFilteringのリクエスト
"""
def generate_IF_request(subject):
    parents, col_name, filter_values = subject
    requests_l = [["in", ["Attribute",k], [v]] for k, v in parents.items() if(v not in ["*","n","n-1"])]
    request = [] if(len(requests_l)==0) else requests_l[0] 
    for next_request in requests_l[1:]:
        request = ["and", request, next_request] if(request!=[]) else next_request
    if(filter_values not in [["*"],["n"],["n-1"]]):
        final_request = ["in",["Attribute",col_name],filter_values]
        request = ["and", request, final_request] if(request!=[]) else final_request
    return request
