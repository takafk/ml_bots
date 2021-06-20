import pytest
from pandas.testing import assert_frame_equal

from lib.compute_pipes.technicals import add_all_ta_without_leak


@pytest.fixture
def compute_ta_features(test_ohlcdata):

    test_ohlc_part = test_ohlcdata.iloc[:100, :].copy(deep=True)

    test_features_part = add_all_ta_without_leak(
        test_ohlc_part,
        open="open",
        high="high",
        low="low",
        close="close",
        volume="volume",
    )

    test_features = add_all_ta_without_leak(
        test_ohlcdata,
        open="open",
        high="high",
        low="low",
        close="close",
        volume="volume",
    )

    return (test_features_part, test_features)


class TestSimpleLeak:
    def test_nan_number(self, compute_ta_features):

        part_features, all_features = compute_ta_features

        assert all(
            all_features.loc[part_features.index, :].isnull().sum()
            == part_features.isnull().sum()
        )

    def test_smoke(self, compute_ta_features):

        part_features, all_features = compute_ta_features

        assert (
            assert_frame_equal(all_features.loc[part_features.index, :], part_features)
            is None
        )
