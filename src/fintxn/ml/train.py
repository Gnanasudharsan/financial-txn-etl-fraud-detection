"""Train the LightGBM fraud model with a strict time-based split and log everything to MLflow.

* split by ``event_date`` (train < train_end <= valid < valid_end <= test); no shuffling
* the first ``BURN_IN_DAYS`` are excluded from training: history-based features (new merchant,
  customer z-score) are meaningless before customers have any history
* class imbalance via ``scale_pos_weight = negatives / positives``
* decision threshold chosen on **validation** as the highest score that still reaches the target
  recall, then frozen and evaluated once on **test**
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)

from fintxn.audit import JobContext
from fintxn.config import PROJECT_ROOT, Settings
from fintxn.gold.features import CATEGORICAL_FEATURES, FEATURE_COLUMNS
from fintxn.gold.job import FEATURES_TABLE
from fintxn.logging import get_logger
from fintxn.storage import Lake

log = get_logger("fintxn.ml.train")
BURN_IN_DAYS = 14
LABEL = "is_fraud"


@dataclass
class SplitData:
    X: pd.DataFrame
    y: np.ndarray


def configure_mlflow(settings: Settings) -> None:
    import mlflow

    mlflow.set_tracking_uri(settings.mlflow.tracking_uri)
    if settings.mlflow.tracking_uri == "databricks":
        mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(settings.mlflow.experiment)


def to_pandas(df: Any) -> SplitData:
    pdf = df.select(*FEATURE_COLUMNS, LABEL).toPandas()
    X = pdf[FEATURE_COLUMNS].astype("float32")
    for c in CATEGORICAL_FEATURES:
        X[c] = X[c].astype("int32")
    return SplitData(X=X, y=pdf[LABEL].to_numpy().astype(int))


def load_splits(lake: Lake) -> tuple[SplitData, SplitData, SplitData, int]:
    version = lake.version("gold", FEATURES_TABLE)
    feats = lake.read("gold", FEATURES_TABLE, version=version)
    burn_in = lake.settings.data.start_date + timedelta(days=BURN_IN_DAYS)
    train = feats.where((F.col("split") == "train") & (F.col("event_date") >= F.lit(burn_in)))
    valid = feats.where(F.col("split") == "valid")
    test = feats.where(F.col("split") == "test")
    return to_pandas(train), to_pandas(valid), to_pandas(test), version


def pick_threshold(y_true: np.ndarray, scores: np.ndarray, target_recall: float) -> float:
    """Highest threshold whose recall on (y_true, scores) is still >= target_recall."""
    precision, recall, thresholds = precision_recall_curve(y_true, scores)
    ok = np.flatnonzero(recall[:-1] >= target_recall)
    if ok.size == 0:
        return float(thresholds.min())
    return float(thresholds[ok.max()])


def evaluate(y: np.ndarray, scores: np.ndarray, threshold: float) -> dict[str, float]:
    pred = (scores >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0, 1]).ravel()
    return {
        "pr_auc": float(average_precision_score(y, scores)),
        "roc_auc": float(roc_auc_score(y, scores)),
        "recall": float(recall_score(y, pred, zero_division=0)),
        "precision": float(precision_score(y, pred, zero_division=0)),
        "f1": float(f1_score(y, pred, zero_division=0)),
        "tp": int(tp),
        "fp": int(fp),
        "fn": int(fn),
        "tn": int(tn),
        "alert_rate": float(pred.mean()),
        "rows": len(y),
        "positives": int(y.sum()),
    }


def _plots(
    outdir: Path,
    booster: lgb.Booster,
    test_eval: dict[str, float],
    y: np.ndarray,
    scores: np.ndarray,
    threshold: float,
) -> list[Path]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    paths: list[Path] = []
    imp = pd.Series(booster.feature_importance("gain"), index=booster.feature_name()).sort_values()
    fig, ax = plt.subplots(figsize=(8, 9))
    imp.plot.barh(ax=ax, color="#3b6ea5")
    ax.set_title("LightGBM feature importance (gain)")
    fig.tight_layout()
    paths.append(outdir / "feature_importance.png")
    fig.savefig(paths[-1], dpi=120)
    plt.close(fig)

    cm = np.array([[test_eval["tn"], test_eval["fp"]], [test_eval["fn"], test_eval["tp"]]])
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.imshow(np.log1p(cm), cmap="Blues")
    for (i, j), v in np.ndenumerate(cm):
        ax.text(j, i, f"{v:,}", ha="center", va="center")
    ax.set_xticks([0, 1], ["pred legit", "pred fraud"])
    ax.set_yticks([0, 1], ["legit", "fraud"])
    ax.set_title(f"Test confusion matrix @ {threshold:.4f}")
    fig.tight_layout()
    paths.append(outdir / "confusion_matrix_test.png")
    fig.savefig(paths[-1], dpi=120)
    plt.close(fig)

    p, r, _ = precision_recall_curve(y, scores)
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.plot(r, p)
    ax.axvline(test_eval["recall"], ls="--", color="grey")
    ax.set_xlabel("recall")
    ax.set_ylabel("precision")
    ax.set_title(f"Test PR curve (AP={test_eval['pr_auc']:.3f})")
    fig.tight_layout()
    paths.append(outdir / "pr_curve_test.png")
    fig.savefig(paths[-1], dpi=120)
    plt.close(fig)
    return paths


def run_train(lake: Lake, ctx: JobContext, results_path: Path | None = None) -> dict[str, Any]:
    import mlflow
    from mlflow.models import infer_signature

    s = lake.settings
    configure_mlflow(s)
    train, valid, test, feat_version = load_splits(lake)
    pos = int(train.y.sum())
    neg = int(len(train.y) - pos)
    spw = neg / max(pos, 1)
    params = {
        **s.model.params,
        "scale_pos_weight": spw,
        "metric": ["average_precision", "auc"],
        "seed": s.data.seed,
    }
    log.info(
        "train=%d (%d fraud) valid=%d test=%d; scale_pos_weight=%.1f",
        len(train.y),
        pos,
        len(valid.y),
        len(test.y),
        spw,
    )

    dtrain = lgb.Dataset(
        train.X, train.y, categorical_feature=CATEGORICAL_FEATURES, free_raw_data=False
    )
    dvalid = lgb.Dataset(
        valid.X, valid.y, categorical_feature=CATEGORICAL_FEATURES, reference=dtrain
    )
    with mlflow.start_run(run_name=f"lgbm-{ctx.run_id[:8]}") as run:
        booster = lgb.train(
            params,
            dtrain,
            num_boost_round=s.model.num_boost_round,
            valid_sets=[dvalid],
            valid_names=["valid"],
            callbacks=[
                lgb.early_stopping(s.model.early_stopping_rounds, verbose=False),
                lgb.log_evaluation(0),
            ],
        )
        v_scores = np.asarray(booster.predict(valid.X, num_iteration=booster.best_iteration))
        threshold = pick_threshold(valid.y, v_scores, s.model.target_recall)
        t_scores = np.asarray(booster.predict(test.X, num_iteration=booster.best_iteration))
        valid_eval = evaluate(valid.y, v_scores, threshold)
        test_eval = evaluate(test.y, t_scores, threshold)

        mlflow.log_params(
            {k: (json.dumps(v) if isinstance(v, list) else v) for k, v in params.items()}
        )
        mlflow.log_params({
            "threshold": threshold, "target_recall": s.model.target_recall,
            "best_iteration": booster.best_iteration, "train_rows": len(train.y),
            "train_end": s.model.train_end.isoformat(), "valid_end": s.model.valid_end.isoformat(),
            "burn_in_days": BURN_IN_DAYS, "features": ",".join(FEATURE_COLUMNS),
        })  # fmt: skip
        mlflow.log_metrics({f"valid_{k}": float(v) for k, v in valid_eval.items()})
        mlflow.log_metrics({f"test_{k}": float(v) for k, v in test_eval.items()})
        mlflow.set_tags({
            "data.gold.features_transactions.version": str(feat_version),
            "data.silver.transactions.version": str(lake.version("silver", "transactions")),
            "pipeline.run_id": ctx.run_id,
        })  # fmt: skip
        with tempfile.TemporaryDirectory() as tmp:
            for p in _plots(Path(tmp), booster, test_eval, test.y, t_scores, threshold):
                mlflow.log_artifact(str(p), artifact_path="plots")
        example = test.X.head(5)
        signature = infer_signature(example, booster.predict(example))
        info = mlflow.lightgbm.log_model(
            booster, artifact_path="model", signature=signature, input_example=example,
            registered_model_name=s.mlflow.model_name,
        )  # fmt: skip
        client = mlflow.MlflowClient()
        mv = info.registered_model_version
        client.set_registered_model_alias(s.mlflow.model_name, "champion", str(mv))
        client.set_model_version_tag(s.mlflow.model_name, str(mv), "threshold", str(threshold))

    importance = pd.Series(booster.feature_importance("gain"), index=booster.feature_name())
    top = (importance / importance.sum()).sort_values(ascending=False).head(10).round(4).to_dict()
    result = {
        "mlflow_run_id": run.info.run_id,
        "model_name": s.mlflow.model_name,
        "model_version": int(mv) if mv is not None else None,
        "threshold": threshold,
        "target_recall": s.model.target_recall,
        "best_iteration": int(booster.best_iteration),
        "scale_pos_weight": round(spw, 2),
        "features_table_version": feat_version,
        "train": {"rows": len(train.y), "positives": pos},
        "valid": valid_eval,
        "test": test_eval,
        "top_features_gain_share": top,
    }
    ctx.rows_in = len(train.y) + len(valid.y) + len(test.y)
    ctx.metrics.update({k: v for k, v in result.items() if k != "top_features_gain_share"})
    if results_path is not None:
        results_path = results_path if results_path.is_absolute() else PROJECT_ROOT / results_path
        results_path.parent.mkdir(parents=True, exist_ok=True)
        results_path.write_text(json.dumps(result, indent=2))
    return result
