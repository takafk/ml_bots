#ライブラリをインポート
import numpy as np
import pandas as pd
import datetime
from datetime import timedelta
import python_bitbankcc

#APIキー，シークレットの設定
API_KEY = 'f533e098-6ef3-4e5b-be45-6976f12d1ec2'
API_SECRET = 'fbb1aebf82470414dcee64381634794647ad8621e180ec44d40d9711250ed717'

#privte API classのオブジェクトを取得
prv = python_bitbankcc.private(API_KEY, API_SECRET)

#注文情報の取得
value = prv.get_order( 'xrp_jpy', '112058661' )#'ペア', '注文ID'


print('取引ID：' + str(value['order_id']))
print('通貨ペア：' + value['pair'])
print('売買情報：' + value['side'])
print('注文タイプ：' + value['type'])
print('注文時の数量：' + value['start_amount'])
print('未約定の数量：' + value['remaining_amount'])
print('約定済の数量：' + value['executed_amount'])
print('注文価格：' + value['price'])
print('平均約定価格：' + value['average_price'])
print('注文状態：' + value['status'])
print('注文日時：' + str(datetime.datetime.fromtimestamp(value['ordered_at']/1000)))
print('約定日時：' + str(datetime.datetime.fromtimestamp(value['executed_at']/1000)))
