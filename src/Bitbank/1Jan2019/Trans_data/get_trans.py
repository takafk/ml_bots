#ライブラリをインポート
import json
import numpy as np
import pandas as pd
import python_bitbankcc
from datetime import datetime as dt
from datetime import timedelta


#パブリックAPIの設定
class BitBankPubAPI:

   #パブリックAPI情報の取得
    def __init__(self):
        self.pub = python_bitbankcc.public()


    #約定価格取得のコード
    def get_transactions(self, pair, date):
        try:
            value = self.pub.get_transactions(pair, date)
            return value
        except Exception as e:
            print(e)
            return None


#日付のリストを作成(datetime.datetimeのリストを返す)

def get_date(start, end):

    strdt = dt.strptime(start, '%Y%m%d')  # 開始日
    enddt = dt.strptime(end, '%Y%m%d')  # 終了日

    # 日付差の日数を算出（リストに最終日も含めたいので、＋１しています）
    days_num = (enddt - strdt).days + 1  # （参考）括弧の部分はtimedelta型のオブジェクトになります

    datelist = []
    for i in range(days_num):
        datelist.append(strdt + timedelta(days=i))

    return datelist

def main():
    pub_set = BitBankPubAPI()

    start = "20181225"
    end = "20181225"
    datelist = get_date(start,end)
    collist = ['Amount','Time','Price','Side','ID']

    for date in datelist:
        trans = pub_set.get_transactions('btc_jpy',date.strftime("%Y%m%d")) #取引通貨ペアの設定
        totdata = pd.DataFrame(trans['transactions'])
        totdata.columns = collist
        totdata['Side'] = 1 * ( totdata['Side'] == 'buy')
        totdata.index = totdata['Time']
        del totdata['Time']

        totdata.to_csv('transdata_' + date.strftime("%Y%m%d") + '.csv')

if __name__ == '__main__':

    main()
