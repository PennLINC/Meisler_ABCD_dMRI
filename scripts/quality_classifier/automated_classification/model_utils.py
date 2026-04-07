"""Shared helpers for automated quality-classifier training and deployment."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterable

import pandas as pd

FEATURE_COLUMNS = [
    "raw_neighbor_corr", "raw_masked_neighbor_corr",
    "raw_dwi_contrast", "raw_num_bad_slices",
    "raw_coherence_index", "raw_incoherence_index",
    "t1_neighbor_corr", "t1_masked_neighbor_corr",
    "t1_dwi_contrast", "t1_num_bad_slices",
    "t1_coherence_index", "t1_incoherence_index",
    "t1post_neighbor_corr", "t1post_masked_neighbor_corr",
    "t1post_dwi_contrast", "t1post_num_bad_slices",
    "t1post_coherence_index", "t1post_incoherence_index",
    "mean_fd", "max_fd", "max_rotation", "max_translation",
    "max_rel_rotation", "max_rel_translation", "t1_dice_distance",
    "CNR0_mean", "CNR1_mean", "CNR2_mean", "CNR3_mean", "CNR4_mean",
    "CNR0_median", "CNR1_median", "CNR2_median", "CNR3_median", "CNR4_median",
    "CNR0_standard_deviation", "CNR1_standard_deviation", "CNR2_standard_deviation",
    "CNR3_standard_deviation", "CNR4_standard_deviation",
]


def _try_read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def resolve_project_root() -> Path:
    """Resolve project root from CONFIG_PATH, nearby config.json, or script location."""
    config_path_str = os.environ.get("CONFIG_PATH")

    if config_path_str:
        cfg_path = Path(config_path_str).expanduser()
        if cfg_path.exists():
            cfg = _try_read_json(cfg_path)
            if cfg and cfg.get("project_root"):
                return Path(cfg["project_root"]).expanduser().resolve()

    for parent in Path(__file__).resolve().parents:
        cfg_path = parent / "config.json"
        if cfg_path.exists():
            cfg = _try_read_json(cfg_path)
            if cfg and cfg.get("project_root"):
                return Path(cfg["project_root"]).expanduser().resolve()
            return parent.resolve()

    return Path(__file__).resolve().parents[3]


def default_data_path() -> Path:
    return resolve_project_root() / "data" / "raw_data" / "merged_data_meisler_analyses.parquet"


def data_dir() -> Path:
    return resolve_project_root() / "data"


def default_cv_dir() -> Path:
    return data_dir() / "quality_classifier" / "cross_validation_results"


def default_final_model_dir() -> Path:
    return data_dir() / "quality_classifier" / "final_pooled_model"


def default_deploy_out_path() -> Path:
    return data_dir() / "raw_data" / "merged_data_meisler_analyses.parquet"


def resolve_output_path(path: Path | None, default_path: Path) -> Path:
    """
    Resolve output paths so outputs stay under project data/ by default.

    - None -> default_path
    - absolute path -> unchanged
    - relative path -> project_root/data/<relative path>
    """
    if path is None:
        return default_path
    if path.is_absolute():
        return path
    return data_dir() / path


def load_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported file type: {path}")


def ensure_subject_session(df: pd.DataFrame) -> pd.DataFrame:
    if "subject_session" in df.columns:
        return df
    if {"subject_id", "session_id"}.issubset(df.columns):
        out = df.copy()
        out["subject_session"] = out["subject_id"].astype(str) + "_" + out["session_id"].astype(str)
        return out
    raise ValueError("Missing subject/session identifiers. Need subject_session or subject_id + session_id.")


def choose_batch_col(df: pd.DataFrame, preferred: str | None = None) -> str:
    candidates = [preferred, "scanner_manufacturer", "Manufacturer", "site"]
    for col in candidates:
        if col and col in df.columns:
            return col
    raise ValueError("No batch/manufacturer column found. Tried: scanner_manufacturer, Manufacturer, site")


def ensure_targets(
    df: pd.DataFrame,
    y_cont_col: str,
    y_bin_col: str,
    pass_threshold: float,
) -> pd.DataFrame:
    out = df.copy()

    if y_cont_col not in out.columns:
        if y_cont_col == "mean_rating_scaled" and "mean_rating" in out.columns:
            out[y_cont_col] = (pd.to_numeric(out["mean_rating"], errors="coerce") + 2.0) / 4.0
        else:
            raise ValueError(
                f"Continuous target '{y_cont_col}' not found and no derivation rule available."
            )

    out[y_cont_col] = pd.to_numeric(out[y_cont_col], errors="coerce")

    if y_bin_col not in out.columns:
        out[y_bin_col] = (out[y_cont_col] >= pass_threshold).astype("Int64")
    out[y_bin_col] = pd.to_numeric(out[y_bin_col], errors="coerce")

    return out


def require_columns(df: pd.DataFrame, columns: Iterable[str], context: str = "data") -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {context}: {missing}")


def prepare_training_data(
    df: pd.DataFrame,
    y_cont_col: str,
    y_bin_col: str,
    pass_threshold: float,
    drop_no_qsiprep_excluded: bool = True,
) -> pd.DataFrame:
    out = ensure_subject_session(df)
    out = ensure_targets(out, y_cont_col=y_cont_col, y_bin_col=y_bin_col, pass_threshold=pass_threshold)

    if drop_no_qsiprep_excluded and "no_qsiprep_exclude" in out.columns:
        out = out[out["no_qsiprep_exclude"] == False]  # noqa: E712

    require_columns(out, FEATURE_COLUMNS + [y_cont_col, y_bin_col, "subject_session"], context="training table")

    for col in FEATURE_COLUMNS + [y_cont_col, y_bin_col]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=FEATURE_COLUMNS + [y_cont_col, y_bin_col, "subject_session"]).copy()
    out[y_bin_col] = out[y_bin_col].astype(int)
    return out
