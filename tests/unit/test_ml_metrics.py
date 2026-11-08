from __future__ import annotations

import numpy as np

from fintxn.ml.train import evaluate, pick_threshold


def test_pick_threshold_reaches_target_recall() -> None:
    rng = np.random.default_rng(0)
    y = (rng.random(5000) < 0.02).astype(int)
    scores = np.clip(y * 0.6 + rng.random(5000) * 0.5, 0, 1)
    t = pick_threshold(y, scores, 0.95)
    recall = ((scores >= t) & (y == 1)).sum() / y.sum()
    assert recall >= 0.95
    # it is the *highest* such threshold: nudging it up drops recall below target
    above = scores[scores > t].min()
    assert ((scores >= above) & (y == 1)).sum() / y.sum() < 0.95


def test_evaluate_confusion_matrix() -> None:
    y = np.array([0, 0, 1, 1, 1, 0])
    s = np.array([0.1, 0.8, 0.9, 0.7, 0.2, 0.3])
    m = evaluate(y, s, 0.5)
    assert (m["tp"], m["fp"], m["fn"], m["tn"]) == (2, 1, 1, 2)
    assert m["recall"] == 2 / 3 and m["precision"] == 2 / 3
    assert m["positives"] == 3 and m["rows"] == 6
