"""
Apply final pooled QC model to merged_data and merge in CV out-of-fold scores
for manually rated scans when available.
"""

from __future__ import annotations

import argparse
import glob
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import load

from model_utils import (
    FEATURE_COLUMNS,
    default_cv_dir,
    default_data_path,
    default_deploy_out_path,
    default_final_model_dir,
    ensure_subject_session,
    load_table,
    resolve_output_path,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deploy pooled QC model on merged_data.")
    parser.add_argument("--data-path", type=Path, default=default_data_path())
    parser.add_argument(
        "--model-path",
        type=Path,
        default=default_final_model_dir() / "final_model_all.joblib",
    )
    parser.add_argument("--cv-dir", type=Path, default=default_cv_dir())
    parser.add_argument("--pass-threshold", type=float, default=0.5)
    parser.add_argument("--out-path", type=Path, default=None)
    return parser


def load_cv_predictions(cv_dir: Path) -> pd.DataFrame | None:
    cv_files = sorted(glob.glob(str(cv_dir / "preds_ALL_*.csv")))
    if len(cv_files) == 0:
        return None

    cv_raw = pd.concat((pd.read_csv(f) for f in cv_files), ignore_index=True)

    id_col = "subject_session" if "subject_session" in cv_raw.columns else "id"
    if id_col not in cv_raw.columns:
        raise ValueError("CV prediction files must contain 'subject_session' or legacy 'id' column.")
    if "y_pred_cont" not in cv_raw.columns:
        raise ValueError("CV prediction files are missing required column: y_pred_cont")

    cv_preds = (
        cv_raw.groupby(id_col, as_index=False)["y_pred_cont"]
        .mean()
        .rename(columns={id_col: "subject_session", "y_pred_cont": "qc_prediction_cv_mean"})
    )
    return cv_preds


def main() -> None:
    args = build_parser().parse_args()
    out_path = resolve_output_path(args.out_path, default_deploy_out_path())

    print(f"[INFO] Loading model from {args.model_path}")
    model = load(args.model_path.expanduser())

    print(f"[INFO] Loading data from {args.data_path}")
    df = ensure_subject_session(load_table(args.data_path.expanduser()))
    print(f"[INFO] Data shape: {df.shape}")

    missing_feats = [f for f in FEATURE_COLUMNS if f not in df.columns]
    if missing_feats:
        raise ValueError(f"Missing required features in data: {missing_feats}")

    cv_preds = load_cv_predictions(args.cv_dir.expanduser())
    if cv_preds is None:
        print("[WARN] No CV prediction files found. Proceeding without OOF overrides.")
        df["qc_prediction_cv_mean"] = np.nan
    else:
        print(f"[INFO] CV-averaged predictions available for {cv_preds.shape[0]} subject-sessions")
        df = df.merge(cv_preds, on="subject_session", how="left")

    if "no_qsiprep_exclude" in df.columns:
        valid_mask = df["no_qsiprep_exclude"] == False  # noqa: E712
    else:
        valid_mask = pd.Series(True, index=df.index)

    df_valid = df.loc[valid_mask].copy()
    x = df_valid[FEATURE_COLUMNS].apply(pd.to_numeric, errors="coerce")
    valid_feature_mask = x.notna().all(axis=1)

    df_predict = df_valid.loc[valid_feature_mask]
    x_predict = x.loc[valid_feature_mask].to_numpy()

    print(f"[INFO] Running pooled model predictions on {len(df_predict)} rows")
    pooled_preds = model.predict(x_predict)

    df["qc_prediction"] = np.nan
    df.loc[df_predict.index, "qc_prediction"] = pooled_preds

    rated_mask = df["qc_prediction_cv_mean"].notna()
    print(f"[INFO] Overriding predictions for {int(rated_mask.sum())} manually rated scans")
    df.loc[rated_mask, "qc_prediction"] = df.loc[rated_mask, "qc_prediction_cv_mean"]

    df["qc_pass_binary"] = np.where(
        df["qc_prediction"].notna(),
        (df["qc_prediction"] >= args.pass_threshold).astype(int),
        np.nan,
    )
    df["manually_rated"] = rated_mask
    df["qc_prediction_source"] = np.where(rated_mask, "cv_oof_mean", "final_pooled_model")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Saving predictions to {out_path}")
    df.to_parquet(out_path, index=False)
    print("[DONE] QC predictions successfully written.")


if __name__ == "__main__":
    main()
