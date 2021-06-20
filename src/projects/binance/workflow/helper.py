from typing import Any, List
import hashlib
import mlflow

from prefect import task


def to_hash(x: Any) -> str:
    """Hasing any objects.
    Returns:
        str: hash value of the input.
    """
    return hashlib.sha256(str(x).encode()).hexdigest()


@task(checkpoint=False)
def store_params(params):
    """Store parameters in MLflow.

    Args:
        params (dict): parameters for storing.
    """
    for k, v in params.items():
        mlflow.log_param(k, v)


@task(checkpoint=False)
def compute_universe(module: Any, univ_name: str, dsmeta: tuple):
    return getattr(module, univ_name).compute(dsmeta)


@task(checkpoint=False)
def read_features(module: Any, features_name: str):
    return getattr(module, features_name)()


@task(checkpoint=False)
def read_label(module: Any, label_name: str, window: int):
    return getattr(module, label_name)(window=window)


@task(checkpoint=False)
def load_target_dsmeta(datastore, symbols: List[str]):
    return (datastore, symbols)
