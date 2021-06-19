from typing import Any
import hashlib
import mlflow

from prefect import task, Flow, Parameter
from prefect import unmapped

from lib2.example_generators import (
    generate_feature,
    generate_features,
    generate_label,
    generate_raw_examples,
)
import projects.binance.features as Features
import projects.binance.labels as Labels
import projects.binance.universe as Universe
from projects.binance.datastore import binance_datastore_latest


def to_hash(x: Any) -> str:
    return hashlib.sha256(str(x).encode()).hexdigest()


@task(checkpoint=False)
def store_params(params):
    for k, v in params.items():
        mlflow.log_param(k, v.default)


@task(checkpoint=False)
def to_params(params: dict) -> dict:
    result = {}
    for k, v in params.items():
        result[k] = Parameter(k, default=v)
    return result


def basic_example_workflow(params: dict) -> Flow:

    with Flow(f"Cleate Examples with {params}.") as flow_crypto_examples:

        # To Parameters in Prefect
        Params: dict = to_params(params)

        # Store parameters in mlflow.
        store_params(Params)

        # Obtain universe.
        datastore = binance_datastore_latest
        total_symbols = datastore.list_symbols()
        univ_pipe = getattr(
            Universe,
            params["universe"],
        )
        symbols = univ_pipe.compute(dsmeta=(datastore, total_symbols))

        # Read raw datasets
        dsmeta = (datastore, symbols)

        # Create features
        feature_pipes = getattr(
            Features,
            params["feature"],
        )()
        start_dt = params["start_dt"]
        end_dt = params["end_dt"]
        features = generate_feature.map(
            dsmeta=unmapped(dsmeta),
            feature_pipe=feature_pipes,
            start_dt=unmapped(start_dt),
            end_dt=unmapped(end_dt),
        )
        df_features = generate_features(features)

        # Create labels
        label_pipe = getattr(Labels, params["label"])(window=params["window"])
        df_label = generate_label(start_dt, end_dt, dsmeta, label_pipe)

        # Create examples
        generate_raw_examples(
            features=df_features,
            label=df_label,
        )

    return flow_crypto_examples
