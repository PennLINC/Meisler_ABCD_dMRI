#!/usr/bin/env python3
"""
Plot Steven metrics using TractVisualizer.

Portable path behavior:
- Resolves project_root from config.json (same pattern as R scripts).
- Reads CSV produced by build_bundle_statistics_csv.R by default:
  {project_root}/data/bundle_statistics.csv
- Supports CLI/env overrides for config, csv, trk_dir, fib_file, output_dir, dsi_studio.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import pandas as pd

from tract_visualizer import TractVisualizer


# Register custom and seaborn colormaps for effect plots
try:
    import seaborn as sns

    _mako = sns.color_palette("mako", as_cmap=True)
    if "mako" not in plt.colormaps():
        plt.colormaps.register(_mako, name="mako")
except Exception:
    pass  # fallback: use viridis if mako not available

_quality_cmap = mcolors.LinearSegmentedColormap.from_list(
    "steven_quality", ["#f7fbff", "#deebf7", "#9ecae1", "#4292c6", "#084594"]
)
if "steven_quality" not in plt.colormaps():
    plt.colormaps.register(_quality_cmap, name="steven_quality")


# Effect columns: column name -> (colorbar title, color_scheme, vmin, vmax)
EFFECT_COLUMNS = {
    # "age_effect_no_quality_NODDI_icvf": ("Age effect, no quality included (NODDI ICVF)", "inferno", 0, 0.52),
    "batch_effect_DKI_mkt": ("Batch effect (DKI MKT)", "mako", 0, 0.698),
    # "quality_effect_contrast_GQI_fa": ("Quality effect (GQI FA)", "steven_quality", 0, 0.138),
}

# Bundle category colors (R bundle_colors -> Python)
BUNDLE_CATEGORY_COLORS = {
    "Association": "#4E79A7",
    "ProjectionBrainstem": "#F28E2B",
    "ProjectionBasalGanglia": "#59A14F",
    "Cerebellum": "#E15759",
    "Commissure": "#B07AA1",
}


def find_config_path(cli_config: str | None) -> Path:
    script_dir = Path(__file__).resolve().parent
    candidates = []

    if cli_config:
        candidates.append(Path(cli_config).expanduser())

    env_config = os.getenv("CONFIG_PATH")
    if env_config:
        candidates.append(Path(env_config).expanduser())

    # Match R script behavior + script-relative fallback
    candidates.extend(
        [
            Path("config.json"),
            Path("..") / "config.json",
            Path("..") / ".." / "config.json",
            script_dir / "config.json",
            script_dir.parent / "config.json",
            script_dir.parent.parent / "config.json",
            script_dir.parent.parent.parent / "config.json",
        ]
    )

    for c in candidates:
        c_abs = c.resolve() if not c.is_absolute() else c
        if c_abs.exists():
            return c_abs

    raise FileNotFoundError("Could not locate config.json. Set --config or CONFIG_PATH.")


def detect_dsi_studio(config: dict, cli_dsi: str | None) -> str | None:
    if cli_dsi:
        return cli_dsi

    if os.getenv("DSI_STUDIO_PATH"):
        return os.getenv("DSI_STUDIO_PATH")

    cfg_dsi = config.get("dsi_studio_path")
    if cfg_dsi:
        return cfg_dsi

    which_dsi = shutil.which("dsi_studio")
    if which_dsi:
        return which_dsi

    # Common macOS app path fallback
    mac_default = "/Applications/dsi_studio.app/Contents/MacOS/dsi_studio"
    if Path(mac_default).exists():
        return mac_default

    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot Steven tract metrics with project-config paths.")
    parser.add_argument("--config", default=None, help="Path to config.json (optional).")
    parser.add_argument("--csv", default=None, help="Path to bundle_statistics.csv (optional).")
    parser.add_argument("--trk-dir", default=None, help="Path to tract directory (optional).")
    parser.add_argument("--fib-file", default=None, help="Path to .fib.gz file (optional).")
    parser.add_argument("--output-dir", default=None, help="Output directory for generated plots.")
    parser.add_argument("--dsi-studio", default=None, help="Path to dsi_studio executable.")
    parser.add_argument("--abbreviations-file", default=None, help="Optional tract abbreviations file path.")
    return parser.parse_args()


def resolve_paths(args: argparse.Namespace) -> dict:
    config_path = find_config_path(args.config)
    config = json.loads(config_path.read_text())

    project_root = Path(config["project_root"]).expanduser().resolve()

    csv_path = Path(args.csv).expanduser().resolve() if args.csv else project_root / "data" / "bundle_statistics.csv"

    # Prefer explicit arg, then common repository location from previous script version,
    # then TractVisualizer defaults if not found.
    trk_dir = None
    if args.trk_dir:
        trk_dir = str(Path(args.trk_dir).expanduser().resolve())
    else:
        candidate_trk = project_root / "data" / "autotrack_2026"
        if candidate_trk.exists():
            trk_dir = str(candidate_trk)

    fib_file = None
    if args.fib_file:
        fib_file = str(Path(args.fib_file).expanduser().resolve())
    elif trk_dir:
        # legacy expected default used by this script
        candidate_fib = Path(trk_dir) / "ICBM152_adult.fib.gz"
        if candidate_fib.exists():
            fib_file = str(candidate_fib)

    output_dir = (
        str(Path(args.output_dir).expanduser().resolve())
        if args.output_dir
        else str((project_root / "figures" / "brain_plots" / "steven_plots").resolve())
    )

    dsi_studio_path = detect_dsi_studio(config, args.dsi_studio)

    abbreviations_file = str(Path(args.abbreviations_file).expanduser().resolve()) if args.abbreviations_file else None

    return {
        "config_path": str(config_path),
        "project_root": str(project_root),
        "csv_path": str(csv_path),
        "trk_dir": trk_dir,
        "fib_file": fib_file,
        "output_dir": output_dir,
        "dsi_studio_path": dsi_studio_path,
        "abbreviations_file": abbreviations_file,
    }


def map_bundles_to_available(df: pd.DataFrame, available_tracts: list[str]) -> pd.DataFrame:
    def _map_bundle_name(bundle_name: str) -> str:
        if bundle_name in available_tracts:
            return bundle_name
        return next(
            (
                t
                for t in available_tracts
                if t == bundle_name or t.endswith("." + bundle_name) or t.endswith(bundle_name)
            ),
            bundle_name,
        )

    mapped = df.copy()
    mapped["bundle"] = mapped["bundle"].apply(_map_bundle_name)
    mapped = mapped[mapped["bundle"].isin(available_tracts)].copy()
    return mapped


def main() -> int:
    args = parse_args()
    paths = resolve_paths(args)

    print(f"Using config: {paths['config_path']}")
    print(f"Project root: {paths['project_root']}")
    print(f"CSV path: {paths['csv_path']}")
    print(f"Output dir: {paths['output_dir']}")

    csv_path = Path(paths["csv_path"])
    if not csv_path.exists():
        raise SystemExit(
            f"Missing CSV: {csv_path}\n"
            "Run build_bundle_statistics_csv.R first or pass --csv."
        )

    Path(paths["output_dir"]).mkdir(parents=True, exist_ok=True)

    viz_kwargs = {
        "root_dir": paths["project_root"],
        "output_dir": paths["output_dir"],
    }
    if paths["trk_dir"]:
        viz_kwargs["trk_dir"] = paths["trk_dir"]
    if paths["fib_file"]:
        viz_kwargs["fib_file"] = paths["fib_file"]
    if paths["dsi_studio_path"]:
        viz_kwargs["dsi_studio_path"] = paths["dsi_studio_path"]
    if paths["abbreviations_file"]:
        viz_kwargs["abbreviations_file"] = paths["abbreviations_file"]

    if "dsi_studio_path" not in viz_kwargs:
        print(
            "Warning: dsi_studio path not found via --dsi-studio, DSI_STUDIO_PATH, config.json, or PATH. "
            "TractVisualizer will use its internal default."
        )

    df = pd.read_csv(csv_path)
    required_cols = {"bundle", "bundle_category"}
    missing = required_cols.difference(df.columns)
    if missing:
        raise SystemExit(f"CSV is missing required columns: {sorted(missing)}")

    viz = TractVisualizer(**viz_kwargs)
    available_tracts = viz.get_available_tracts()

    if not available_tracts:
        raise SystemExit("No available tracts detected by TractVisualizer. Check trk_dir/abbreviations setup.")

    df = map_bundles_to_available(df, available_tracts)
    if df.empty:
        raise SystemExit(
            "No bundles from CSV matched available tracts. "
            "Check bundle names and tract directory settings."
        )

    for col, (_, _, _, _) in EFFECT_COLUMNS.items():
        if col not in df.columns:
            print(f"Skipping {col}: column not in CSV")
        else:
            print(f"{col}: range [{df[col].min()}, {df[col].max()}]")

    base_output = Path(paths["output_dir"])

    # 1) Effect plots: all_tracts
    for col, (colorbar_title, color_scheme, vmin, vmax) in EFFECT_COLUMNS.items():
        if col not in df.columns:
            continue
        out_dir = base_output / col
        out_dir.mkdir(parents=True, exist_ok=True)
        print(f"Plotting {col} ({color_scheme}, range [{vmin}, {vmax}]) -> {out_dir}")
        viz.visualize_tracts(
            tract_df=df,
            tract_name_column="bundle",
            values_column=col,
            plot_mode="all_tracts",
            color_scheme=color_scheme,
            output_name=col,
            output_dir=str(out_dir),
            colorbar=True,
            colorbar_title=colorbar_title,
            value_range_override=(vmin, vmax),
        )

    # 2) By category: grid of each individual tract (left + bilateral)
    category_dir = base_output / "by_category"
    category_dir.mkdir(parents=True, exist_ok=True)

    for cat, hex_color in BUNDLE_CATEGORY_COLORS.items():
        cat_df = df[df["bundle_category"] == cat].copy()
        if cat_df.empty:
            continue

        def _is_left_or_bilateral(tract_name: str) -> bool:
            _, _, sub = viz._find_tract_file(tract_name)
            return sub in ("left_hem", "bilateral")

        tract_list = [t for t in cat_df["bundle"].tolist() if _is_left_or_bilateral(t)]
        if not tract_list:
            continue

        cat_df = cat_df[cat_df["bundle"].isin(tract_list)].copy()
        print(f"Category {cat}: grid of tracts (left + bilateral, n={len(tract_list)}) -> {category_dir}")
        viz.visualize_tracts(
            tract_df=cat_df,
            tract_name_column="bundle",
            tract_list=tract_list,
            single_color=hex_color,
            plot_mode="iterative",
            tract_gradient_plot=True,
            view="lateral",
            output_name=f"category_{cat}_grid",
            output_dir=str(category_dir),
            colorbar=False,
        )

    # 3) By category: one all_tracts plot per bundle_category
    for cat, hex_color in BUNDLE_CATEGORY_COLORS.items():
        cat_df = df[df["bundle_category"] == cat].copy()
        if cat_df.empty:
            continue
        cat_df["_color"] = hex_color
        print(f"Category {cat} -> {category_dir}")
        viz.visualize_tracts(
            tract_df=cat_df,
            tract_name_column="bundle",
            color_column="_color",
            plot_mode="all_tracts",
            output_name=f"category_{cat}",
            output_dir=str(category_dir),
            colorbar=False,
        )

    print("Done. Outputs under:", base_output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
