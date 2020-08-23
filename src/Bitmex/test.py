import calendar
import ccxt
import pandas as pd
import numpy as np
import requests

from sklearn import linear_model
from datetime import datetime
from time import sleep


# For test

# bitmex = ccxt.bitmex({
#    'apiKey': 'z5nYzkF_6drLZ8s5uQKJkS6y',
#    'secret': 'EkVXpJk-5_LUMn9xWSETzVQPuUiwzgtTH4s7AUcMCEDr3eH0',
# })

# bitmex.urls['api'] = bitmex.urls['test']


# Notification to LINE


def LineNotify(message):
    line_notify_token = "T94z9vZaVnMHjU5VRtv6xYUnixwNTwmAMHglIbPchr6"
    line_notify_api = "https://notify-api.line.me/api/notify"

    payload = {"message": message}
    headers = {"Authorization": "Bearer " + line_notify_token}
    requests.post(line_notify_api, data=payload, headers=headers)


message_error = "Errorですよ。Botは止まってしまいました。"


bitmex = ccxt.bitmex(
    {
        "apiKey": "RR40xxve7kWPcwh6HUiJP7jX",
        "secret": "sCymtpUSnZkSCP_shOKy5CArW41oGsB9a118_DmZsplkrjvC",
    }
)


ERROR_TIME = 20


# Latest Price


def lastprice(pair):
    value = bitmex.fetch_ticker(pair)["last"]
    return value


# Get Historical DataFrame


def get_historicaldata():

    # Parameters
    minutes = 800  # Length of data for sample data

    # Obtain dataset
    now = datetime.utcnow()  # Current time
    unixtime = calendar.timegm(now.utctimetuple())  # Unit time of current time
    since = (unixtime - 60 * minutes) * 1000  # Start unix time
    ohlcvList = pd.DataFrame(bitmex.fetch_ohlcv("BTC/USD", "1m", since, limit=minutes))
    ohlcvList.columns = ["datetime", "open", "high", "low", "close", "volume"]

    # calculate recent return
    ohlcvList["return"] = ohlcvList.pct_change()["close"]

    return ohlcvList


# Laterst standard deviation


def calculate_sigma(data):

    # Parameters
    minutes = 30  # Length of data for calculating sigma

    twosigma = 2 * np.std(data["close"][-minutes:-1].values)

    return twosigma


# Calculate Conditioning Variables


def current_volume_pressure(delta):

    # Hyperparamer
    diff_ratio = 0.4

    # Return variable
    side = "neutral"  # parameter for current volume pressure

    # Get data
    latest_order_book = bitmex.fetch_order_book(symbol="BTC/USD")

    bids = np.array(latest_order_book["bids"])
    asks = np.array(latest_order_book["asks"])

    INT_DETLA = int(delta / 0.5)

    bids_volume = bids[:INT_DETLA, 1].sum()
    asks_volume = asks[:INT_DETLA, 1].sum()
    volume_pressure = bids_volume / asks_volume

    if volume_pressure < diff_ratio:
        side = "sell"
    elif volume_pressure > 1 + diff_ratio:
        side = "buy"

    return side


# Calculate prediction


def judge_condtion(data):

    q10 = np.quantile(data, 0.1)  # 10% quantile point
    q25 = np.quantile(data, 0.25)  # 25% quantile point
    q75 = np.quantile(data, 0.75)  # 75% quantile point
    q90 = np.quantile(data, 0.9)  # 90% quantile point
    state = 0

    if (data[-1] < q10) or (data[-1] > q90):
        state = 1  # Momentrum regime
    else:
        if (data[-1] > q10) and (data[-1] < q25):
            state = 0  # Mean reversion
        elif (data[-1] > q75) and (data[-1] < q90):
            state = 0  # Mean reversion
        else:
            state = 1  # Momentrum regime

    return state


def calculate_prediction(data):

    # Hyper parameters, computed by analyis on jupyter notebook
    Momentum_window = 10
    reversion_window = 5
    predict_min = 3  # Target time horizon
    time_prev = 30  # Define Minutes for calculating hisitorical volatility

    # Return variables
    condition = "momentum"
    side = "neutral"

    # Caculate current market condition
    data["histvol_prev"] = data["return"].rolling(time_prev).std().values
    data["condition"] = (
        data["histvol_prev"].rolling(time_prev).apply(judge_condtion, raw=True)
    )
    if int(data.iloc[-1]["condition"]) == 1:
        condition = "momentum"
    else:
        condition = "reversion"

    # Feature creation
    if condition == "reversion":  # Mean reversion
        data["feature"] = -1 * data["close"].rolling(window=reversion_window).apply(
            lambda x: (x[-1] - x[0]) / x[0], raw=True
        ).shift(predict_min)
    else:  # Momentum
        data["feature"] = (
            data["return"].rolling(window=Momentum_window).mean().shift(predict_min)
        )

    # Calculate predictor using linear regression

    try:
        clf = linear_model.LinearRegression()
        if condition == "reversion":
            df_reversion = data[["return", "feature"]].dropna()
            X = df_reversion["return"].values.reshape(-1, 1)
            Y = df_reversion["feature"].values.reshape(-1, 1)
        else:
            df_momentum = pd.DataFrame(data[data["condition"] == 1])
            df_momentum = df_momentum[["return", "feature"]].dropna()
            X = df_momentum["return"].values.reshape(-1, 1)
            Y = df_momentum["feature"].values.reshape(-1, 1)

    except Exception as e:
        print(e)

    clf.fit(X, Y)
    predictor = clf.predict(X)

    if predictor[-1][0] > 0:
        side = "buy"
    else:
        side = "sell"

    return (condition, side, predict_min)


# Functions for order


def order_limit(pair, lot, side, size, targetprice, delta):

    if bitmex.fetch_balance()["BTC"]["total"] > lot:
        error_times = 0

        while error_times < ERROR_TIME:
            try:
                if side == "buy":
                    price = targetprice - delta
                elif side == "sell":
                    price = targetprice + delta
                params = {
                    "execInst": "ParticipateDoNotInitiate",
                }
                value = bitmex.create_order(
                    pair,
                    type="limit",
                    side=side,
                    amount=size,
                    price=price,
                    params=params,
                )
                print("limit order activate: {}".format(side))
                error_times = ERROR_TIME
            except Exception as e:
                print(e)
                error_times = error_times + 1
                break
    else:
        print("残高が足りません。")
        return 0


def order_stop_limit(pair, lot, side, size, tagetprice, trigger):

    if bitmex.fetch_balance()["BTC"]["total"] > lot:

        error_times = 0
        while error_times < ERROR_TIME:
            try:
                if side == "buy":
                    trigger_price = tagetprice + trigger
                    price = trigger_price + 0.5

                elif side == "sell":
                    trigger_price = tagetprice - trigger
                    price = trigger_price - 0.5
                params = {
                    "stopPx": trigger_price,
                    "execInst": "ParticipateDoNotInitiate,LastPrice",
                }
                value = bitmex.create_order(
                    pair,
                    type="StopLimit",
                    side=side,
                    amount=size,
                    price=price,
                    params=params,
                )
                print("stop order activate: {}".format(side))
                error_times = ERROR_TIME
            except Exception as e:
                print(e)
                error_times = error_times + 1

    else:
        print("残高が足りません。")
        return 0


def order_stop_market(pair, lot, side, size, tagetprice, trigger):

    if bitmex.fetch_balance()["BTC"]["total"] > lot:

        error_times = 0
        while error_times < ERROR_TIME:
            try:
                if side == "buy":
                    trigger_price = tagetprice + trigger

                elif side == "sell":
                    trigger_price = tagetprice - trigger

                params = {
                    "stopPx": trigger_price,
                }
                value = bitmex.create_order(
                    pair, type="stop", side=side, amount=size, params=params
                )
                print("stop order activate: {}".format(side))
                error_times = ERROR_TIME
            except Exception as e:
                print(e)
                error_times = error_times + 1

    else:
        print("残高が足りません。")
        return 0


def cancel_allorders():

    orders = bitmex.fetch_open_orders()
    error_times = 0

    while error_times < ERROR_TIME:
        try:
            for order in orders:
                cancel = bitmex.cancel_order(order["id"])
            error_times = ERROR_TIME
        except Exception as e:
            print(e)
            error_times = error_times + 1


# Main

if __name__ == "__main__":

    # Paramter

    PAIR = "BTC/USD"
    LOT = 0.0001
    SIZE = 50  # Size of order, unit is $
    WAITING_TIME = 3  # Interval of bot's action, unit is sec
    i = 0

    while True:

        try:

            # Cancel all current orders
            cancel_allorders()

            # Calculate dynamical parameters
            TRIGGER = 10  # Trigger price difference from order price, unit is $, initial is 10$
            DELTA_CLEARPOS = (
                1  # Order price difference from last price, unit is $, iniital is 10$
            )
            DELTA_HAVPOS = (
                0.5  # Order price difference from last price, unit is $, iniital is 10$
            )

            # Current status
            last_price = lastprice(PAIR)
            positions = bitmex.private_get_position()
            position_size = positions[0]["currentQty"]
            position_price = positions[0]["avgEntryPrice"]
            print("current position: {}".format(position_size))
            print("2σ: {}".format(TRIGGER))

            if position_size > 0:  # Long position
                print("entry price: {}".format(position_price))
                if last_price >= position_price + DELTA_CLEARPOS:  # Profitable
                    order_limit(PAIR, LOT, "sell", abs(position_size), last_price, 0.5)
                    sleep(WAITING_TIME)
                else:  # Nonprofitable
                    if (int(position_price) - TRIGGER) > last_price:
                        order_stop_market(
                            PAIR,
                            LOT,
                            "sell",
                            abs(position_size),
                            int(position_price),
                            TRIGGER,
                        )
                        sleep(WAITING_TIME)
                    else:
                        order_limit(
                            PAIR,
                            LOT,
                            "sell",
                            abs(position_size),
                            int(position_price),
                            DELTA,
                        )
                        order_stop_limit(
                            PAIR,
                            LOT,
                            "sell",
                            abs(position_size),
                            int(position_price),
                            TRIGGER,
                        )
                        sleep(WAITING_TIME)

            elif position_size < 0:  # Short Position
                print("entry price: {}".format(position_price))
                if last_price <= position_price - DELTA_CLEARPOS:  # Profitable
                    order_limit(PAIR, LOT, "buy", abs(position_size), last_price, 0.5)
                    sleep(WAITING_TIME)
                else:  # Non Profitable
                    if (
                        int(position_price) + TRIGGER
                    ) < last_price:  # Immediately clear position
                        order_stop_market(
                            PAIR,
                            LOT,
                            "buy",
                            abs(position_size),
                            int(position_price),
                            TRIGGER,
                        )
                        sleep(WAITING_TIME)
                    else:
                        order_limit(
                            PAIR,
                            LOT,
                            "buy",
                            abs(position_size),
                            int(position_price),
                            DELTA,
                        )
                        order_stop_limit(
                            PAIR,
                            LOT,
                            "buy",
                            abs(position_size),
                            int(position_price),
                            TRIGGER,
                        )
                        sleep(WAITING_TIME)

            else:  # Neutral Position

                # Get Historical data
                data = get_historicaldata()

                # Calculate parameters
                TRIGGER = int(calculate_sigma(data))
                DELTA = max([1, TRIGGER // 10])

                # Calculate Conditioning Variable of Current Pressure
                current_pressure = current_volume_pressure(DELTA)

                # Determine position
                if current_pressure != "neutral":
                    side = current_pressure
                    print(current_pressure + "pressure")
                else:
                    market_condition, side, predict_min = calculate_prediction(data)
                    print(market_condition)
                    print(side)

                order_limit(PAIR, LOT, side, SIZE, last_price, DELTA_HAVPOS)

                # Limit order
                if side == "buy":
                    order_stop_limit(PAIR, LOT, "sell", SIZE, last_price, TRIGGER)
                else:
                    order_stop_limit(PAIR, LOT, "buy", SIZE, last_price, TRIGGER)

                sleep(WAITING_TIME)

        except Exception as e:
            print("error: {0}".format(e))
            if i < 100000:
                sleep(3)
            else:
                LineNotify(message_error)
                break
