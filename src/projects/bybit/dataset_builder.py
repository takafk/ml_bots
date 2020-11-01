with Flow("Cleate Examples for Crypto.") as flow_crypto_examples:

    # Parameters for generating examples.
    crypto_list = Parameter("crypto_list", default=params['crypto_list'])
    min_symbols = Parameter("min_symbols", default=len(params['crypto_list']))

    # Read raw datasets
    reader = Reader(result=WAREHOUSE)
    dfmetas = reader.map(keys=crypto_list)

    # Create features
    feature_pipes = Parameter("feature_pipes", default=FEATURE_PIPES)

    feature_byassets = generate_features_byassets.map(
        dfmetas, feature_pipes=unmapped(feature_pipes))

    features = create_features(feature_byassets)

    # Create labels
    label_pipes = Parameter("label_pipes", default=params['label_pipes'])
    window = Parameter("window", default=params['window'])
    demean = Parameter("demean", default=params['demean'])

    label_byassets = generate_label_byassets.map(
        dfmetas, label_pipes=unmapped(label_pipes))

    labels = create_labels(label_byassets, window=window, demean=demean)

    # Create examples
    num_of_features = Parameter(
        "num_of_features", default=params['num_of_features'])
    hash_value = Parameter("hash_value", default=params['hash_value'])
    is_classification = Parameter(
        "is_classification", default=params['is_classification'])

    raw_examples = generate_raw_examples(
        features=features, labels=labels, hash_value=hash_value, min_symbols=min_symbols)

    selected_examples = ic_selection(
        raw_examples=raw_examples, num_of_features=num_of_features)

    examples = post_label_transform(
        selected_examples, is_classification=is_classification, hash_value=hash_value)
