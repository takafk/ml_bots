from typing import Dict, Any


from sklearn.preprocessing import LabelEncoder


def symbol_cat(examples: Dict[str, Any]):
    """ Add categorical feature of symbols.
    """

    # LabelEncoder
    le = LabelEncoder()
    # fit label
    le = le.fit(examples["symbol_cat"].values)
    # label to integer
    examples["symbol_cat"] = le.transform(examples["symbol_cat"].values)
