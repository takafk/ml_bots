from typing import Any, List
from joblib import Parallel, delayed
import pandas as pd
import numpy as np

from sklearn.utils.validation import _deprecate_positional_args, _check_fit_params
from sklearn.base import is_classifier, clone
from sklearn.utils import indexable
from sklearn.model_selection._split import check_cv
from sklearn.model_selection._validation import _enforce_prediction_order
from sklearn.preprocessing import LabelEncoder

from .metrics import compute_metrics


def convert_to_int(X: pd.DataFrame, y: pd.Series):
    """Convert datetime of MultiIndex to int for CPCV.

    Args:
        X (pd.DataFrame): DataFrame with multiIndex.
        y (pd.Series): [description]: pd.Series with MultiIndex. Label of X.
    """
    # LabelEncoder
    le = LabelEncoder()

    assert all(X.index == y.index)

    assert y.name == "label"

    # Convert from timestamp to int
    le.fit(X.index.get_level_values(0))
    index_num = le.transform(X.index.get_level_values(0))

    X_cov = X.reset_index().iloc[:, 2:]
    y_cov = y.reset_index().iloc[:, 2:]["label"]

    X_cov.index = index_num
    y_cov.index = index_num

    return (X_cov, y_cov)


def _safe_split(X: pd.DataFrame, y: pd.Series, indices: List[int]):
    """Create subset of dataset and properly handle kernels."""
    X_subset = X.loc[indices, :]

    y_subset = y.loc[indices]

    return X_subset, y_subset


def fit_and_predict(estimator, X, y, train, test, verbose, fit_params, method):
    """Fit estimator and predict values for a given dataset split.

    Args:
        estimator ([type]): model.
        X (pd.DataFrame): in-sample features.
        y (pd.Series): in-sample label.
        train (list): index of train data.
        test (list): index of test data.
        verbose (int): logging level for joblib.
        fit_params (dict): parameters of model
        method (str): prediction type. Default is "predict_proba".

    Returns:
        prediction: predictions for test data.
        test: index of test data.
    """

    # Adjust length of sample weights
    fit_params = fit_params if fit_params is not None else {}
    fit_params = _check_fit_params(X, fit_params, train)

    X_train, y_train = _safe_split(X, y, train)
    X_test, _ = _safe_split(X, y, test)

    if y_train is None:
        estimator.fit(X_train, **fit_params)
    else:
        estimator.fit(X_train, y_train, **fit_params)
    func = getattr(estimator, method)
    predictions = func(X_test)

    encode = False
    # encode = method in ['decision_function', 'predict_proba',
    #                    'predict_log_proba'] and y is not None

    if encode:
        if isinstance(predictions, list):
            predictions = [
                _enforce_prediction_order(
                    estimator.classes_[i_label],
                    predictions[i_label],
                    n_classes=len(set(y[:, i_label])),
                    method=method,
                )
                for i_label in range(len(predictions))
            ]
        else:
            # A 2D y array should be a binary label indicator matrix
            n_classes = len(set(y)) if y.ndim == 1 else y.shape[1]
            predictions = _enforce_prediction_order(
                estimator.classes_, predictions, n_classes, method
            )
    return predictions, test


@_deprecate_positional_args
def cross_val_partial_predict(
    estimator,
    X,
    y=None,
    *,
    groups=None,
    cv=None,
    n_jobs=None,
    verbose=0,
    fit_params=None,
    pre_dispatch="2*n_jobs",
    method="predict_proba"
):
    """Obtain prediction for a single validation data of Cross Validation.

    Args:
        estimator ([type]): model.
        X (pd.DataFrame): Feature Matrix.
        y (pd.Series): Label series.
        groups ([type], optional): [description]. Defaults to None.
        cv (Any, optional): Type of cross validation. Defaults to None.
        n_jobs (int, optional): number of cpus for parallel computing. Defaults to None.
        verbose (int, optional): option for logging in parallel computing. Defaults to 0.
        fit_params ([type], optional): parameters for model. Defaults to None.
        pre_dispatch (str, optional): See the explanation in sklearn. Defaults to '2*n_jobs'.
        method (str, optional): . Defaults to 'predict_proba'.

    Returns:
        val_indices (list): List of predictions for validation datas.
        predictions (list): List of indices of validation datas.

    Examples:
        cv = cpcv.split(X_is_cov, y_is_cov)
        val_indices, val_predictions_proba = cross_val_partial_predict(
            clf_raw, X_is_cov, y_is_cov, cv=cv, method='predict_proba')
    """

    X, y, groups = indexable(X, y, groups)

    cv = check_cv(cv, y, classifier=is_classifier(estimator))

    # We clone the estimator to make sure that all the folds are
    # independent, and that it is pickle-able.
    parallel = Parallel(n_jobs=n_jobs, verbose=verbose, pre_dispatch=pre_dispatch)
    prediction_blocks = parallel(
        delayed(fit_and_predict)(
            clone(estimator), X, y, train, test, verbose, fit_params, method
        )
        for train, test in cv.split(X, y, groups)
    )

    # Concatenate the predictions
    predictions = [pred_block_i for pred_block_i, _ in prediction_blocks]
    val_indices = [indices_i for _, indices_i in prediction_blocks]

    return val_indices, predictions


def _to_pred_from_proba(probas: np.array, ave_pred: np.array):
    """From probability distribution to expected value.

    Args:
        probas (np.array): probability for each class.
        ave_pred (np.array): range of each class.

    Returns:
        float: expected value
    """

    assert len(probas[0]) == len(ave_pred)

    return (probas * ave_pred).mean(axis=1)


def cross_val_metrics_multindex(
    y_true: pd.Series,
    val_indices: List[List[int]],
    val_y_preds: List[List[float]],
    q_class: int,
    cost: float,
):
    """Compute metrics for all cross validation paths.

    Args:
        y_true (pd.Series): label for all index.
        val_indices (List[List[int]]): List of indices for validations.
        val_y_preds (List[List[float]]): List of predictions for validations.
    """

    # Same number of validation paths
    assert len(val_indices) == len(val_y_preds)

    metrics = []

    for val_indice, val_y_pred in zip(val_indices, val_y_preds):

        val_y_pred_series: pd.Series = pd.Series(
            val_y_pred, index=y_true[val_indice].index
        )
        val_y_true: pd.Series = y_true[val_indice]

        metrics.append(
            compute_metrics(val_y_true, val_y_pred_series, q_class=q_class, cost=cost)
        )

    return metrics


def evaluation_with_cross_validation(
    model: Any,
    cv: Any,
    X: pd.DataFrame,
    y_class: pd.Series,
    y_pred: pd.Series,
    ave_pred: List[float],
    cost: float = 0.05 / 100,
):
    """Evaluate a model with Cross validation.

    Args:
        model (Any): target model.
        cv (Any): cross validation.
        X (pd.DataFrame): Feature matrix.
        y_class (pd.Series): Label of class.
        y_pred (pd.Series): Label of value.
        ave_pred (np.array): range of each label class.
        cost (float): cost for one transaction.

    Returns:
        metrics (pd.DataFrame): Metrics of validations.

    Examples:
        PURGING_WINDOW = 10
        cpcv = CPCV_Multindex(window=WINDOW + OFF_SET + PURGING_WINDOW)
        metrics = evaluation_with_cpcv(model, cv=cpcv, X, y_class, y_pred, ave_pred)
    """

    X_cov, y_cov = convert_to_int(X, y_class)

    cv_generator = cv.split(X_cov, y_cov)
    val_indices, val_predictions_proba = cross_val_partial_predict(
        model, X_cov, y_cov, cv=cv_generator, method="predict_proba"
    )

    val_predictions = [
        _to_pred_from_proba(predicition_proba, ave_pred)
        for predicition_proba in val_predictions_proba
    ]

    _, y_pred_true = convert_to_int(X, y_pred[y_class.index].rename("label"))

    val_metrics = cross_val_metrics_multindex(
        y_pred_true,
        val_indices,
        val_predictions,
        q_class=5,
        cost=cost,
    )

    return pd.DataFrame(val_metrics)
