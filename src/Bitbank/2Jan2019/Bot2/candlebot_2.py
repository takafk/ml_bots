#ライブラリをインポート
import numpy as np
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta

#　データの整形
def mod_data(data, mindif, maxdif):

    data['Mid'] = (data['Max'] + data['Min']) // 2
    data['Difindex'] = 1 * ( ( mindif < data['End'] - data['Start']) & (data['End'] - data['Start'] < maxdif ) ) -  1 * (  data['End'] - data['Start'] < 0)
    buylist = [0,0]
    for i in range(2,len(data['Start'])):
        buylist.append(data.at[data.index[i],'Difindex'] + data.at[data.index[i-1],'Difindex'])

    list = pd.DataFrame({'Buyindex':buylist}, index=data.index)
    data = data.join([list], how='left')

    data['Higeindex'] = 1 * (  ((data['Max'] - data['End'])/(data['End'] - data['Start']) >=  0 ) & ( (data['Max'] - data['End'])/(data['End'] - data['Start']) < 0.2 ) )

    return data

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


#お金を稼ぐBotのクラス
class Millions_bot:

    def __init__(self, time):
        self.time = time #ロスカする時間
        self.position = 0 # ポジションを持っているかを表すIndex
        self.price = 0 #持っているポジションを買った価格
        self.remtime = time #ポジションを持った場合の残り時間
        self.staytime = 0 #利確したあとに待つ時間
        self.totprofit = 0 #トータルの利益
        self.profit_list = [0,0] #トータルの利益のヒストリカル推移

    def calculate_profit(self, data):

        i = 2

        while i < len(data['Start']):
            if self.staytime == 0:
                if ((self.position == 0 and data.at[data.index[i-1], 'Buyindex'] == 2) and (data.at[data.index[i-1], 'Higeindex'] * data.at[data.index[i-2], 'Higeindex'] == 1)): #ポジション無しかつ上昇トレンド
                    self.position = 1
                    self.price = data.at[data.index[i], 'Start']
                elif self.position == 1: # ポジションあり(売却or様子を見るの2択)。
                    if data.at[data.index[i], 'Start'] > self.price: # 買値より高ければ、売却して利確
                        self.totprofit +=  data.at[data.index[i], 'Start'] - self.price
                        self.position = 0
                        self.remtime = self.time
                        self.staytime = 5 #利確したら、すごし間をおく
                    elif (data.at[data.index[i], 'Start'] < self.price): #買値より安い場合は、様子を見るか損切り
                        if self.remtime > 0: # 待ち時間がある
                            if data.at[data.index[i], 'Start'] - self.price < -500: #200円以上下がったら、すぐに損切り(急落と判断)
                                self.totprofit +=  data.at[data.index[i], 'Start'] - self.price
                                self.position = 0
                                self.remtime = self.time
                            else: #下降トレンドにはないと判断
                                self.remtime = self.remtime - 1
                        else: # 損切りする(待ち時間が0)
                            self.totprofit +=  data.at[data.index[i], 'Mid'] - self.price
                            self.position = 0
                            self.remtime = self.time
                i += 1
                self.profit_list.append(self.totprofit)
            else:
                self.staytime += -1
                self.profit_list.append(self.totprofit)
                i += 1



def main():

    mindif = 1000
    maxdif = 10000
    holdingtime = 10
    start = '20180125'
    end = '20181225'
    datelist = get_date(start,end)
    PLlist = []

    for date in datelist:

        datestr = date.strftime("%Y%m%d")

        # 取引データの読み込みと修正
        path = '/Users/takahisa/Desktop/Bot/Candle_data/'
        data = pd.read_csv(path + 'candledata_' + datestr + '.csv', index_col='Time')
        data = mod_data(data, mindif, maxdif)

        #ボットへデータのフィット
        bot = Millions_bot(holdingtime)
        bot.calculate_profit(data)

        #　利益の結果をCSVファイルへ
        pdresult = pd.DataFrame({'PL':bot.profit_list}, index=data.index)
        data = data.join([pdresult], how='left')
        data.to_csv('./Result/PL_hist' + datestr + '.csv')
        PLlist.append(bot.totprofit)

    PLlist = pd.DataFrame({'FinalPL':PLlist}, index = datelist)
    PLlist.to_csv('./Result/FinalPL.csv')


if __name__ == '__main__':

    main()
