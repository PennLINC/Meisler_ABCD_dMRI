#!/usr/bin/env python
"""
Train the final pooled XGBoost regressor on manually rated scans.

Defaults are project-native and derived from merged_data parquet.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import dump
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold
from skopt import BayesSearchCV
from skopt.space import Integer, Real
from xgboost import XGBRegressor

from model_utils import (
    FEATURE_COLUMNS,
    choose_batch_col,
    default_data_path,
    default_final_model_dir,
    load_table,
    prepare_training_data,
    resolve_output_path,
)

SEARCH_SPACE = {
    "n_estimators": Integer(50, 5000),
    "min_child_weight": Integer(1, 10),
    "gamma": Real(1e-3, 5.0, prior="log-uniform"),
    "eta": Real(1e-3, 0.5, prior="log-uniform"),
    "subsample": Real(0.2, 1.0),
    "colsample_bytree": Real(0.1, 1.0),
    "max_depth": Integer(2, 8),
}


def compute_binary_metrics(y_true_bin: np.ndarray, y_pred_cont: np.ndarray, pass_threshold: float):
    y_pred_bin = (y_pred_cont >= pass_threshold).astype(int)
    auc = roc_auc_score(y_true_bin, y_pred_cont) if len(np.unique(y_true_bin)) > 1 else np.nan
    acc = accuracy_score(y_true_bin, y_pred_bin)
    cm = confusion_matrix(y_true_bin, y_pred_bin, labels=[0, 1])
    tn, fp, fn, tp = cm.ravel() if cm.size == 4 else (np.nan, np.nan, np.nan, np.nan)
    sens = tp / (tp + fn) if (tp + fn) else np.nan
    spec = tn / (tn + fp) if (tn + fp) else np.nan
    return auc, acc, sens, spec


def train_and_evaluate_pooled_model(
    df: pd.DataFrame,
    y_cont_col: str,
    y_bin_col: str,
    batch_col: str,
    pass_threshold: float,
    seed: int,
    n_splits: int,
    n_iter: int,
):
    x = df[FEATURE_COLUMNS].to_numpy(dtype=float)
    y_cont = df[y_cont_col].astype(float).to_numpy()
    y_bin = df[y_bin_col].astype(int).to_numpy()
    batches = df[batch_col].astype(str).to_numpy()

    inner_cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=seed)

    reg = XGBRegressor(
        nthread=4,
        objective="reg:squarederror",
        eval_metric="rmse",
        random_state=seed,
        tree_method="hist",
    )

    print(f"[INFO] Running {n_splits}-fold BayesSearchCV ({n_iter} iterations)...")
    opt = BayesSearchCV(
        reg,
        SEARCH_SPACE,
        n_iter=n_iter,
        scoring="neg_root_mean_squared_error",
        cv=inner_cv.split(x, y_bin),
        random_state=seed,
        verbose=0,
    )
    opt.fit(x, y_cont)
    best_model = opt.best_estimator_
    best_params = opt.best_params_

    outer_cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=seed)
    preds, trues, bins, batch_vals = [], [], [], []
    for train_idx, test_idx in outer_cv.split(x, y_bin):
        x_train, x_test = x[train_idx], x[test_idx]
        y_train, y_test = y_cont[train_idx], y_cont[test_idx]
        model = XGBRegressor(**best_model.get_params())
        model.fit(x_train, y_train)
        y_pred = model.predict(x_test)
        preds.append(y_pred)
        trues.append(y_test)
        bins.append(y_bin[test_idx])
        batch_vals.append(batches[test_idx])

    preds = np.concatenate(preds)
    trues = np.concatenate(trues)
    bins = np.concatenate(bins)
    batch_vals = np.concatenate(batch_vals)

    results = []
    auc, acc, sens, spec = compute_binary_metrics(bins, preds, pass_threshold)
    rmse = float(np.sqrt(mean_squared_error(trues, preds)))
    mae = mean_absolute_error(trues, preds)
    r2 = r2_score(trues, preds)
    results.append(
        {
            "batch": "ALL",
            "N": int(len(trues)),
            "RMSE": rmse,
            "MAE": mae,
            "R2": r2,
            "AUC": auc,
            "Accuracy": acc,
            "Sensitivity": sens,
            "Specificity": spec,
            "batch_col": batch_col,
        }
    )

    for b in np.unique(batch_vals):
        idx = batch_vals == b
        if idx.sum() < 10:
            continue
        auc, acc, sens, spec = compute_binary_metrics(bins[idx], preds[idx], pass_threshold)
        rmse = float(np.sqrt(mean_squared_error(trues[idx], preds[idx])))
        mae = mean_absolute_error(trues[idx], preds[idx])
        r2 = r2_score(trues[idx], preds[idx])
        results.append(
            {
                "batch": b,
                "N": int(idx.sum()),
                "RMSE": rmse,
                "MAE": mae,
                "R2": r2,
                "AUC": auc,
                "Accuracy": acc,
                "Sensitivity": sens,
                "Specificity": spec,
                "batch_col": batch_col,
            }
        )

    metrics_df = pd.DataFrame(results)
    return best_model, best_params, metrics_df


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train final pooled QC model.")
    parser.add_argument("--data-path", type=Path, default=default_data_path())
    parser.add_argument("--outdir", type=Path, default=default_final_model_dir())
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--n-splits", type=int, default=5)
    parser.add_argument("--n-iter", type=int, default=200)
    parser.add_argument("--y-cont-col", default="mean_rating_scaled")
    parser.add_argument("--y-bin-col", default="rating_binary")
    parser.add_argument("--batch-col", default="scanner_manufacturer")
    parser.add_argument("--pass-threshold", type=float, default=0.5)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    outdir = resolve_output_path(args.outdir, default_final_model_dir())

    print(f"[INFO] Loading data: {args.data_path}")
    raw_df = load_table(args.data_path.expanduser())
    df = prepare_training_data(
        raw_df,
        y_cont_col=args.y_cont_col,
        y_bin_col=args.y_bin_col,
        pass_threshold=args.pass_threshold,
    )
    batch_col = choose_batch_col(df, preferred=args.batch_col)

    print(f"[INFO] Training on {len(df)} rated scans, batch_col={batch_col}, seed={args.seed}")
    best_model, best_params, metrics_df = train_and_evaluate_pooled_model(
        df,
        y_cont_col=args.y_cont_col,
        y_bin_col=args.y_bin_col,
        batch_col=batch_col,
        pass_threshold=args.pass_threshold,
        seed=args.seed,
        n_splits=args.n_splits,
        n_iter=args.n_iter,
    )

    outdir.mkdir(parents=True, exist_ok=True)
    dump(best_model, outdir / "final_model_all.joblib")
    with open(outdir / "final_model_best_params.json", "w", encoding="utf-8") as f:
        json.dump(best_params, f, indent=2)
    metrics_df.to_csv(outdir / "final_model_metrics.csv", index=False)

    print("\n[INFO] === Final Model Performance ===")
    print(metrics_df.to_string(index=False))
    print(f"\n[DONE] Saved artifacts to {outdir}")


if __name__ == "__main__":
    main()
