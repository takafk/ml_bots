import pytest
import pandas as pd
from numpy.random import normal


@pytest.fixture
def test_ohlcdata():
    """Test data for simple leak checking.

    Returns:
        pd.DataFrame: OHLC dataframe.
    """

    Length = 200

    test_ohlcdata = {
        "open": [100 + normal(0, 1) for i in range(Length)],
        "high": [120 + normal(0, 1) for i in range(Length)],
        "low": [80 + normal(0, 1) for i in range(Length)],
        "close": [90 + normal(0, 1) for i in range(Length)],
        "volume": [1000 + normal(0, 100) for i in range(Length)],
    }

    return pd.DataFrame(
        test_ohlcdata, index=pd.date_range(start="19/09/2012", periods=Length, freq="D")
    )
