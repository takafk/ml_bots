# https://qiita.com/YuukiMiyoshi/items/42a890a95af6ab7a5348　を参考にした

# ライブラリのインポート
from datetime import datetime as dt
from datetime import timedelta

# 日付条件の設定
std = "20180224"
endd = "20180305"
strdt = dt.strptime(std, "%Y%m%d")  # 開始日
enddt = dt.strptime(endd, "%Y%m%d")  # 終了日

# 日付差の日数を算出（リストに最終日も含めたいので、＋１しています）
days_num = (enddt - strdt).days + 1  # （参考）括弧の部分はtimedelta型のオブジェクトになりま

datelist = []
for i in range(days_num):
    datelist.append(strdt + timedelta(days=i))

for d in datelist:
    print(d.strftime("%Y%m%d"))
