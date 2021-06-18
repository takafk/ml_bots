import projects.binance.features as features
import projects.binance.labels as labels
import projects.binance.universe as universe


def to_hash(x: str) -> str:
    return hashlib.sha256(str(x).encode()).hexdigest()

@task(checkpoint=False)
def store_params(params: dict):
    for k: str, v : Parameter in params.items():
        mlflow.log_param(k, v)


@task(checkpoint=False)
def to_params(params: dict) -> dict:
    result = {}
    for k, v in params.items():
        result[k] = Parameter(k, default=v)


default_params = {
    "window": 30,
    "universe": "LIQUIDITYTOP20",
    "label": "fwd_return",
    "feature": "BASIC_RETURNS",
    "start_dt": "2021-05-28",
    "end_dt": "2021-06-17",
}

default_params["hash_value"] = to_hash(default_params.items())
default_params["hash_value_features"] = to_hash(default_params["feature_pipes"])
default_params["hash_value_universe"] = to_hash(default_params["symbols"])


def example_workflow(dsmeta: dict, params_org: dict) -> Flow:

    with Flow(f"Cleate Examples with {params_org}.") as flow_crypto_examples:

        params = to_params(params_org)


        window = Parameter(
            "window",
            default=params["window"],
        )
        univ = 

        label_pipe = getattr(labels, params["label"])(window)
        feature_pipes = getattr(features, params["feature"])()

        store_params(params_org)

        # Parameters for caching.
        hash_value = Parameter(
            "hash_value",
            default=params_org["hash_value"],
        )
        hash_value_features = Parameter(
            "hash_value_features",
            default=params_org["hash_value_features"],
        )
        hash_value_universe = Parameter(
            "hash_value_universe",
            default=params_org["hash_value_universe"],
        )


        # Read raw datasets
        dsmeta = Parameter("dsmeta", default=dsmeta_org)

        # Create features
        start_dt = Parameter("start_dt", default=params["start_dt"])
        end_dt = Parameter("end_dt", default=params["end_dt"])
        features = generate_feature.map(
            dsmeta=unmapped(dsmeta),
            feature_pipe=params_org["feature_pipes"],
            start_dt=unmapped(start_dt),
            end_dt=unmapped(end_dt),
        )
        df_features = generate_features(features)

        # Create labels

        df_label = generate_label(start_dt, end_dt, dsmeta, params_org["label_pipe"])

        # Create examples
        raw_examples = generate_raw_examples(
            features=df_features,
            label=df_label,
        )

    return flow_crypto_examples