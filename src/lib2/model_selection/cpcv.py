from typing import Tuple, Iterable, List
from dataclasses import dataclass, field
import itertools as itt
import pandas as pd
import numpy as np


@dataclass(frozen=True)
class CPCV:
    """Combinatorial Purged Cross Validation"""

    n_folds: int = field(default=10)
    k_groups: int = field(default=2)
    window: int = field(default=2)

    def split(
        self,
        X: pd.DataFrame,
        y: pd.Series,
    ) -> Iterable[Tuple[np.ndarray, np.ndarray]]:

        indices = list(set(X.index))

        fold_bounds = [
            (fold.index[0], fold.index[-1]) for fold in np.array_split(X, self.n_folds)
        ]

        # Generate list of (start_test_dt, end_test_dt) for test data
        test_folds_bounds = list(itt.combinations(fold_bounds, self.k_groups))
        test_folds_bounds.reverse()

        for test_folds_bound in test_folds_bounds:

            test_indices = self._test_indices(indices, test_folds_bound)

            train_indices = self._train_indices(indices, test_indices)

            # Purging
            train_indices = self._purging(
                test_folds_bound, train_indices, window=self.window
            )

            yield train_indices, test_indices

    def _test_indices(self, indices, test_folds_bound: List[Tuple]):

        test_indices = np.empty(0)

        for test_fold_bound in test_folds_bound:

            test_indices = np.union1d(
                test_indices, indices[test_fold_bound[0] : test_fold_bound[-1]]
            ).astype(int)

        return test_indices

    def _train_indices(self, indices, test_indices):

        train_indices = np.setdiff1d(indices, test_indices)

        return train_indices

    def _purging(self, test_folds_bound, train_indices, window):

        purged = np.empty(0)

        for start_ix, end_ix in test_folds_bound:
            purged = np.union1d(purged, np.arange(start_ix, start_ix + window))
            purged = np.union1d(purged, np.arange(end_ix - window, end_ix))

        train_indices = np.setdiff1d(train_indices, purged.astype(int))

        return train_indices
