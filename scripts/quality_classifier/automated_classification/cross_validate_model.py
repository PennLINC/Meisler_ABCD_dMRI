"""
Nested CV (5 outer x 5 inner) with XGBoost regression for manual QC ratings.

Default behavior is project-native:
- Input: merged_data parquet from data/raw_data
- ID: subject_session
- Batch stratification: scanner_manufacturer (fallbacks supported)
- Targets:
  - mean_rating_scaled (derived from mean_rating if missing)
  - rating_binary (derived from mean_rating_scaled >= pass-threshold if missing)
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd
import shap
from scipy.stats import pearsonr, spearmanr
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    log_loss,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import Pipeline
from skopt import BayesSearchCV
from skopt.space import Integer, Real
from xgboost import XGBRegressor

from model_utils import (
    FEATURE_COLUMNS,
    choose_batch_col,
    default_cv_dir,
    default_data_path,
    load_table,
    prepare_training_data,
    resolve_output_path,
)

SEARCH_SPACE = {
    "reg__n_estimators": Integer(50, 5000),
    "reg__min_child_weight": Integer(1, 10),
    "reg__gamma": Real(1e-3, 5.0, prior="log-uniform"),
    "reg__eta": Real(1e-3, 0.5, prior="log-uniform"),
    "reg__subsample": Real(0.2, 1.0),
    "reg__colsample_bytree": Real(0.1, 1.0),
    "reg__max_depth": Integer(2, 8),
}


def _build_strat_labels_with_batch(df: pd.DataFrame, y_bin_col: str, batch_col: str, n_splits: int) -> pd.Series:
    labels = df[y_bin_col].astype(str) + "__" + df[batch_col].astype(str)
    if (labels.value_counts() < n_splits).any():
        labels = df[y_bin_col].astype(int)
    return labels


def run_one_seed_regression_across_all(
    df: pd.DataFrame,
    seed: int,
    y_cont_col: str,
    y_bin_col: str,
    batch_col: str,
    pass_threshold: float,
    outer_splits: int,
    inner_splits: int,
    n_iter: int,
):
    x_df = df[FEATURE_COLUMNS].astype(float)
    y_cont = df[y_cont_col].astype(float).to_numpy()
    y_bin = df[y_bin_col].astype(int).to_numpy()
    ids = df["subject_session"].astype(str).to_numpy()

    strat_labels = _build_strat_labels_with_batch(df, y_bin_col=y_bin_col, batch_col=batch_col, n_splits=outer_splits)
    outer_cv = StratifiedKFold(n_splits=outer_splits, shuffle=True, random_state=seed)

    preds_rows, metrics_rows, shap_rows, shap_rank_rows = [], [], [], []

    for fold_id, (train_idx, test_idx) in enumerate(outer_cv.split(x_df, strat_labels)):
        x_train_df, x_test_df = x_df.iloc[train_idx], x_df.iloc[test_idx]
        y_train, y_test = y_cont[train_idx], y_cont[test_idx]
        ybin_test = y_bin[test_idx]
        ids_test = ids[test_idx]

        inner_cv = StratifiedKFold(n_splits=inner_splits, shuffle=True, random_state=seed)
        inner_labels = strat_labels.iloc[train_idx]

        pipe = Pipeline([
            (
                "reg",
                XGBRegressor(
                    nthread=4,
                    random_state=seed,
                    objective="reg:squarederror",
                    eval_metric="rmse",
                    tree_method="auto",
                ),
            )
        ])

        opt = BayesSearchCV(
            pipe,
            SEARCH_SPACE,
            n_iter=n_iter,
            scoring="neg_root_mean_squared_error",
            cv=inner_cv.split(x_train_df, inner_labels),
            random_state=seed,
            verbose=0,
            return_train_score=False,
        )
        opt.fit(x_train_df, y_train)
        best_pipe = opt.best_estimator_

        y_pred_cont = best_pipe.predict(x_test_df)
        y_pred_bin = (y_pred_cont >= pass_threshold).astype(int)

        rmse = float(np.sqrt(mean_squared_error(y_test, y_pred_cont)))
        mae = mean_absolute_error(y_test, y_pred_cont)
        r2 = r2_score(y_test, y_pred_cont)
        try:
            r_pear, p_pear = pearsonr(y_test, y_pred_cont)
        except Exception:
            r_pear, p_pear = (np.nan, np.nan)
        try:
            r_spear, p_spear = spearmanr(y_test, y_pred_cont)
        except Exception:
            r_spear, p_spear = (np.nan, np.nan)

        try:
            auc = roc_auc_score(ybin_test, y_pred_cont)
        except Exception:
            auc = np.nan
        acc = accuracy_score(ybin_test, y_pred_bin)
        cm = confusion_matrix(ybin_test, y_pred_bin, labels=[0, 1])
        tn, fp, fn, tp = cm.ravel() if cm.size == 4 else (np.nan, np.nan, np.nan, np.nan)
        sens = tp / (tp + fn) if (tp + fn) else np.nan
        spec = tn / (tn + fp) if (tn + fp) else np.nan
        y_prob_clipped = np.clip(y_pred_cont, 0, 1)
        try:
            ll = log_loss(ybin_test, y_prob_clipped, labels=[0, 1])
        except Exception:
            ll = np.nan

        metrics_rows.append(
            {
                "batch": "ALL",
                "seed": seed,
                "fold": fold_id,
                "RMSE": rmse,
                "MAE": mae,
                "R2": r2,
                "Pearson_r": r_pear,
                "Pearson_p": p_pear,
                "Spearman_r": r_spear,
                "Spearman_p": p_spear,
                "AUC": auc,
                "Accuracy": acc,
                "Sensitivity": sens,
                "Specificity": spec,
                "LogLoss": ll,
                "n_test": len(test_idx),
                "batch_col": batch_col,
            }
        )

        fold_preds = pd.DataFrame(
            {
                "subject_session": ids_test,
                "batch_value": df.iloc[test_idx][batch_col].astype(str).values,
                "seed": seed,
                "fold": fold_id,
                "y_true_cont": y_test,
                "y_true_binary": ybin_test,
                "y_pred_cont": y_pred_cont,
                "y_pred_binary": y_pred_bin,
            }
        )
        preds_rows.append(fold_preds)

        reg = best_pipe.named_steps["reg"]
        explainer = shap.TreeExplainer(reg)
        shap_vals = explainer.shap_values(x_test_df.to_numpy())

        shap_df = pd.DataFrame(shap_vals, columns=FEATURE_COLUMNS)
        shap_df["subject_session"] = ids_test
        shap_df["batch_value"] = fold_preds["batch_value"].values
        shap_df["seed"] = seed
        shap_df["fold"] = fold_id
        shap_rows.append(shap_df)

        mean_abs = np.abs(shap_vals).mean(axis=0)
        ranks = pd.Series(mean_abs, index=FEATURE_COLUMNS).rank(ascending=False, method="min")
        shap_rank_rows.append(
            pd.DataFrame(
                {
                    "feature": FEATURE_COLUMNS,
                    "mean_abs_shap": mean_abs,
                    "rank": ranks.values,
                    "batch": "ALL",
                    "seed": seed,
                    "fold": fold_id,
                }
            )
        )

    preds_df = pd.concat(preds_rows, ignore_index=True)
    metrics_df = pd.DataFrame(metrics_rows)
    shap_values_df = pd.concat(shap_rows, ignore_index=True)
    shap_ranks_df = pd.concat(shap_rank_rows, ignore_index=True)
    return preds_df, metrics_df, shap_values_df, shap_ranks_df


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Nested CV quality classifier training.")
    parser.add_argument("--data-path", type=Path, default=default_data_path())
    parser.add_argument("--outdir", type=Path, default=default_cv_dir())
    parser.add_argument("--y-cont-col", default="mean_rating_scaled")
    parser.add_argument("--y-bin-col", default="rating_binary")
    parser.add_argument("--batch-col", default="scanner_manufacturer")
    parser.add_argument("--pass-threshold", type=float, default=0.5)
    parser.add_argument("--n-seeds", type=int, default=1000)
    parser.add_argument("--n-outer-splits", type=int, default=5)
    parser.add_argument("--n-inner-splits", type=int, default=5)
    parser.add_argument("--n-iter", type=int, default=200)
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Explicit seed to run. If omitted, uses SLURM_ARRAY_TASK_ID within 0..n-seeds-1.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    outdir = resolve_output_path(args.outdir, default_cv_dir())

    print(f"[INFO] Loading data: {args.data_path}")
    raw_df = load_table(args.data_path.expanduser())
    df = prepare_training_data(
        raw_df,
        y_cont_col=args.y_cont_col,
        y_bin_col=args.y_bin_col,
        pass_threshold=args.pass_threshold,
    )
    batch_col = choose_batch_col(df, preferred=args.batch_col)

    if args.seed is None:
        task_id = int(os.environ.get("SLURM_ARRAY_TASK_ID", 0))
        if task_id < 0 or task_id >= args.n_seeds:
            raise IndexError(f"Task id {task_id} out of range 0..{args.n_seeds - 1}")
        seed = task_id
    else:
        seed = args.seed

    print(
        f"[INFO] Running pooled nested CV: n={len(df)}, seed={seed}, "
        f"target={args.y_cont_col}, batch_col={batch_col}"
    )

    preds, metrics, shap_vals, shap_ranks = run_one_seed_regression_across_all(
        df=df,
        seed=seed,
        y_cont_col=args.y_cont_col,
        y_bin_col=args.y_bin_col,
        batch_col=batch_col,
        pass_threshold=args.pass_threshold,
        outer_splits=args.n_outer_splits,
        inner_splits=args.n_inner_splits,
        n_iter=args.n_iter,
    )

    outdir.mkdir(parents=True, exist_ok=True)
    preds.to_csv(outdir / f"preds_ALL_{seed}.csv", index=False)
    metrics.to_csv(outdir / f"metrics_ALL_{seed}.csv", index=False)
    shap_vals.to_csv(outdir / f"shap_ALL_{seed}.csv", index=False)
    shap_ranks.to_csv(outdir / f"shap_ranks_ALL_{seed}.csv", index=False)

    print(f"[DONE] Wrote outputs to {outdir} for seed {seed}")


if __name__ == "__main__":
    main()
