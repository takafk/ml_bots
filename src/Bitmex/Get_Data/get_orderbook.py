import calendar
import ccxt
import pandas as pd
import numpy as np
import requests

from sklearn import linear_model
from datetime import datetime
from time import sleep


# For test

#bitmex = ccxt.bitmex({
#    'apiKey': 'z5nYzkF_6drLZ8s5uQKJkS6y',
#    'secret': 'EkVXpJk-5_LUMn9xWSETzVQPuUiwzgtTH4s7AUcMCEDr3eH0',
#})

#bitmex.urls['api'] = bitmex.urls['test']


# Notification to LINE

def LineNotify(message):
    line_notify_token = "T94z9vZaVnMHjU5VRtv6xYUnixwNTwmAMHglIbPchr6"
    line_notify_api = "https://notify-api.line.me/api/notify"

    payload = {"message":message}
    headers = {"Authorization":"Bearer " + line_notify_token}
    requests.post(line_notify_api, data = payload, headers = headers)

message_error = 'Errorですよ。データ取得が死にました。'




bitmex = ccxt.bitmex({
       'apiKey': 'RR40xxve7kWPcwh6HUiJP7jX',
       'secret': 'sCymtpUSnZkSCP_shOKy5CArW41oGsB9a118_DmZsplkrjvC',
})



ERROR_TIME = 20


def get_orderbook():

    error_times = 0
    max_error = 20

    while error_times < max_error:

        try:
            # Get data
            latest_order_book = bitmex.fetch_order_book(symbol='BTC/USD')
            print('Sucess in getting order book')
            error_times = max_error
        except Exception as e:
            print(e)
            error_times = error_times + 1

    return(latest_order_book)


# Get Historical DataFrame

if __name__ == '__main__':

    # Paramter
    i = 0
    result = []
    counter = 0
    INTERVAL = 60 # sec
    LOG_INTERBAL = 60 # min

    while True:

        try:
            if counter % LOG_INTERBAL == 0:
                pd.DataFrame(result).to_csv('book_history_{}.csv'.format(counter))
                print('Logged the book history.')
                result = []
            now = datetime.utcnow() # Current time
            book = get_orderbook()
            result.append([now, book])
            counter += 1
            sleep(INTERVAL)

        except Exception as e:
            print("error: {0}".format(e))
            if i < 100000:
                sleep(3)
            else:
                LineNotify(message_error)
                break
