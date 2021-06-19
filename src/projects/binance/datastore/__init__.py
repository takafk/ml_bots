# flake8: noqa
from .base import BinanceLocalDataStore

binance_datastore_latest = BinanceLocalDataStore(
    path="/home/jovyan/work/local_disks/data_warehouse/binance/datastore.h5"
)
