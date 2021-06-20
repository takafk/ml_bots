from typing import List
import pandas as pd

from prefect import Flow, Parameter
from prefect import unmapped

from core import ComputePipe
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
from .helper import (
    to_hash,
    store_params,
    compute_universe,
    read_features,
    read_label,
    load_target_dsmeta,
)


def basic_example_workflow(params_org: dict) -> Flow:

    params = params_org.copy()
    params["hash_value"] = to_hash(params_org.items())

    with Flow(f"Cleate Examples with {params}.") as flow_crypto_examples:

        # Store parameters in mlflow.
        store_params(params)

        # Obtain universe.
        datastore = binance_datastore_latest
        total_symbols: List[str] = datastore.list_symbols()
        universe = Parameter("universe", default=params["universe"])
        symbols: List[str] = compute_universe(
            Universe, universe, dsmeta=(datastore, total_symbols)
        )

        # Create datastore with metadata (symbols).
        dsmeta = load_target_dsmeta(datastore, symbols)

        # Create features
        feature = Parameter("feature", default=params["feature"])
        feature_pipes: List[ComputePipe] = read_features(Features, feature)
        start_dt: str = params["start_dt"]
        end_dt: str = params["end_dt"]
        features = generate_feature.map(
            dsmeta=unmapped(dsmeta),
            feature_pipe=feature_pipes,
            start_dt=unmapped(start_dt),
            end_dt=unmapped(end_dt),
        )
        df_features: pd.DataFrame = generate_features(features)

        # Create labels
        label = Parameter("label", default=params["label"])
        window = Parameter("window", default=params["time_horizon"])
        label_pipe: ComputePipe = read_label(Labels, label, window=window)
        df_label = generate_label(start_dt, end_dt, dsmeta, label_pipe)

        # Create examples
        hash_value = Parameter("hash_value", default=params["hash_value"])
        generate_raw_examples(
            features=df_features,
            label=df_label,
            hash_value=hash_value,
        )

    return flow_crypto_examples
