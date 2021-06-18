import lib2.compute_factory as cf


def BASIC_RETURNS():
    return (
        cf.Return(window=30),
        cf.Return(window=60),
        cf.Return(window=120),
        cf.Return(window=240),
    )


def BASIC_MADIV():
    return (
        cf.MADiv(window=30),
        cf.MADiv(window=60),
        cf.MADiv(window=120),
        cf.MADiv(window=240),
    )


def BASIC_VOLATILITY():
    return (
        cf.Volatility(window=30),
        cf.Volatility(window=60),
        cf.Volatility(window=120),
        cf.Volatility(window=240),
    )


def BASIC_VWAP():
    return (
        cf.VWAP(window=30),
        cf.VWAP(window=60),
        cf.VWAP(window=120),
        cf.VWAP(window=240),
    )


def BASIC_SUPPORT():
    return (
        cf.Support(window=30),
        cf.Support(window=60),
        cf.Support(window=120),
        cf.Support(window=240),
    )


def BASIC_RESISTANCE():
    return (
        cf.Resistance(window=30),
        cf.Resistance(window=60),
        cf.Resistance(window=120),
        cf.Resistance(window=240),
    )


def BASIC_MADIV_LIQUIDITY():
    return (
        cf.MADiv(base=cf.Liquidity(), window=30),
        cf.MADiv(base=cf.Liquidity(), window=60),
        cf.MADiv(base=cf.Liquidity(), window=120),
    )


def BASIC_MARKET():
    return (
        cf.Extend(
            series=cf.MADiv(base=cf.MarketReturn(), window=30),
            base=cf.Return(),
        ),
        cf.Extend(
            series=cf.MADiv(base=cf.MarketReturn(), window=60),
            base=cf.Return(),
        ),
        cf.Extend(
            series=cf.MADiv(base=cf.MarketReturn(), window=120),
            base=cf.Return(),
        ),
        cf.Extend(
            series=cf.Volatility(base=cf.MarketReturn(), window=30),
            base=cf.Return(),
        ),
        cf.Extend(
            series=cf.Volatility(base=cf.MarketReturn(), window=60),
            base=cf.Return(),
        ),
        cf.Extend(
            series=cf.Volatility(base=cf.MarketReturn(), window=120),
            base=cf.Return(),
        ),
    )


def BASIC_HIGHIC():
    return (
        cf.Return(window=30),
        cf.Volatility(window=120),
        cf.VWAP(window=60),
        cf.Support(window=30),
        cf.Resistance(window=60),
    )


def BASIC_HIGHIC_NOVOL():
    return (
        cf.Return(window=30),
        cf.VWAP(window=60),
        cf.VWAP(window=240),
        cf.Support(window=30),
        cf.Resistance(window=60),
    )
