#!/usr/bin/env python3
"""Tract visualizer for DSI Studio bundle image generation."""

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image

# Try to import tm_utils for custom colormaps
try:
    sys.path.append(os.path.join(os.path.dirname(__file__)))
    import tm_utils

    # Get custom colormaps
    _, _, _, _, _, fds_cmap = tm_utils.make_colormaps()
    HAS_TM_UTILS = True
except ImportError:
    print("Warning: Could not import tm_utils. fds_cmap will not be available.")
    fds_cmap = None
    HAS_TM_UTILS = False


class TractVisualizer:
    """
    DSI Studio tract visualization with DataFrame input.
    
    Uses numbered tract files (like the R script approach) to ensure proper loading order
    and color mapping. Takes a DataFrame with tract names, values for sorting/coloring,
    and optional custom RGB colors. Can also take a list of tract names and visualize them.
    """
    # Supported tract file extensions (DSI Studio can load both)
    TRACT_EXTENSIONS = ('.trk.gz', '.tt.gz')

    @staticmethod
    def _find_config_path(config_path: Optional[str] = None) -> Optional[Path]:
        """Find config.json using CLI/env/cwd/script-relative fallbacks."""
        script_dir = Path(__file__).resolve().parent
        candidates = []

        if config_path:
            candidates.append(Path(config_path).expanduser())

        env_config = os.getenv("CONFIG_PATH")
        if env_config:
            candidates.append(Path(env_config).expanduser())

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

        for candidate in candidates:
            candidate_abs = candidate.resolve() if not candidate.is_absolute() else candidate
            if candidate_abs.exists():
                return candidate_abs
        return None

    @staticmethod
    def _read_project_config(config_path: Optional[str] = None) -> Dict:
        """Load config.json when available; return empty dict otherwise."""
        cfg_path = TractVisualizer._find_config_path(config_path=config_path)
        if cfg_path is None:
            return {}
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    @staticmethod
    def _resolve_dsi_studio_path(explicit_path: Optional[str], config: Dict) -> str:
        """Resolve DSI Studio path with portable fallbacks."""
        if explicit_path:
            return explicit_path

        env_path = os.getenv("DSI_STUDIO_PATH")
        if env_path:
            return env_path

        cfg_path = config.get("dsi_studio_path")
        if cfg_path:
            return cfg_path

        which_path = shutil.which("dsi_studio")
        if which_path:
            return which_path

        return "/Applications/dsi_studio.app/Contents/MacOS/dsi_studio"

    def __init__(self, root_dir: Optional[str] = None,
                 trk_dir: Optional[str] = None,
                 dsi_studio_path: Optional[str] = None,
                 output_dir: Optional[str] = None,
                 fib_file: Optional[str] = None,
                 abbreviations_file: Optional[str] = None,
                 config_path: Optional[str] = None):
        """
        Initialize the tract visualizer.
        
        Parameters:
        -----------
        root_dir : Optional[str]
            Root directory for the project. If None, attempts to resolve from config.json
            (key: project_root), otherwise falls back to current working directory.
        trk_dir : Optional[str]
            Directory containing tract files (.trk.gz or .tt.gz) (default: {root_dir}/data/raw/tracts_trk)
        dsi_studio_path : Optional[str]
            Path to DSI Studio executable (default: /Applications/dsi_studio.app/Contents/MacOS/dsi_studio)
        output_dir : Optional[str]
            Directory for output visualizations (default: {root_dir}/results/tract_visualization)
        fib_file : Optional[str]
            Path to .fib.gz file for DSI Studio (default: {trk_dir}/HCP1065.1mm.fib.gz)
        abbreviations_file : Optional[str]
            Path to abbreviations file for tract name mapping (.xlsx or .csv). If not provided or file missing/empty, tract names are inferred from filenames in trk_dir (left_hem/right_hem).
        config_path : Optional[str]
            Optional explicit path to config.json used to resolve project_root and dsi_studio_path.
        """
        config = self._read_project_config(config_path=config_path)

        resolved_root = root_dir if root_dir is not None else config.get("project_root", os.getcwd())
        self.root_dir = str(Path(resolved_root).expanduser().resolve())
        
        # Cache for filesystem-discovered tracts (used when no abbreviations file)
        self._discovered_tracts: Optional[List[str]] = None
        
        # Set paths with defaults if not provided
        self.trk_dir = trk_dir if trk_dir is not None else f'{self.root_dir}/data/raw/tracts_trk'
        self.dsi_studio_path = self._resolve_dsi_studio_path(dsi_studio_path, config)
        self.output_dir = output_dir if output_dir is not None else f'{self.root_dir}/results/tract_visualization'
        self.fib_file = fib_file if fib_file is not None else f'{self.trk_dir}/HCP1065.1mm.fib.gz'
        self.abbreviations_file = abbreviations_file if abbreviations_file is not None else f'{self.root_dir}/data/derivatives/tract_names/abbreviations.xlsx'
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Load tract abbreviations
        self.abbreviations = self._load_abbreviations()
        
    def get_available_tracts(self) -> List[str]:
        """Get list of all available tract names that can be visualized.
            
        Returns:
        --------
        List[str]
            List of all available tract names (canonical filename stems from abbreviations or trk_dir)
        """
        return self._get_all_tracts_from_abbreviations()
    
    def validate_tract_names(self, tract_list: List[str]) -> Dict[str, List[str]]:
        """Validate tract names and return matching status.
        
        Parameters:
        -----------
        tract_list : List[str]
            List of tract identifiers to validate
            
        Returns:
        --------
        Dict[str, List[str]]
            Dictionary with keys:
            - 'valid': List of tract names that were successfully matched
            - 'invalid': List of tract names that could not be matched
        """
        valid_tracts = []
        invalid_tracts = []
        
        for tract in tract_list:
            matched = self._match_tract_names([tract])
            if matched:
                valid_tracts.extend(matched)
            else:
                invalid_tracts.append(tract)
        
        return {
            'valid': valid_tracts,
            'invalid': invalid_tracts
        }
    
    def visualize_tracts(self, tract_df: Optional[pd.DataFrame] = None,
                        values_column: Optional[str] = None,
                        tract_name_column: str = 'Tract_Short_Name',
                        tract_list: Optional[Union[List[str], str]] = None,
                        color_scheme: str = 'coolwarm',
                        color_column: Optional[str] = None,
                        single_color: Optional[Union[str, Tuple[float, float, float], List[Union[str, Tuple[float, float, float]]]]] = None,
                        plot_mode: str = 'iterative',
                        view: Union[str, List[str]] = 'lateral',
                        grid_orientation: Optional[str] = None,
                        tract_gradient_plot: bool = False,
                        gradient_n_tracts: int = 10,
                        colorbar: bool = False,
                        colorbar_title: Optional[str] = None,
                        output_name: str = 'tract_visualization',
                        output_dir: Optional[str] = None,
                        keep_csv: bool = False,
                        keep_color_files: bool = False,
                        cleanup_renamed_files: bool = True,
                        include_bilateral: bool = True,
                        value_range_override: Optional[Tuple[float, float]] = None) -> None:
        """
        Visualize tracts using DSI Studio.
        Hemisphere is automatically detected from tract names.
        Uses numbered tract files (like R script approach) to ensure proper loading order.
        
        Parameters:
        -----------
        tract_df : Optional[pd.DataFrame]
            DataFrame containing tract information with optional columns:
            - Tract name column (specified by tract_name_column)  
            - Values column (specified by values_column) for sorting and coloring
            - Color column (specified by color_column) for per-tract colors
            Required when values_column, color_column, or custom tract_name_column is used.
            Optional when using tract_list with single_color (default: None)
        values_column : Optional[str]
            Name of the column containing values for sorting and coloring (e.g., 'Gini_Coefficient').
            Requires tract_df to be provided. Optional if single_color is specified (default: None)
        tract_name_column : str
            Name of the column containing tract names in tract_df (default: 'Tract_Short_Name')
            Supports same formats as _match_tract_names: Tract_Long_Name, Tract, Abbreviation, 
            new_qsirecon_tract_names, HCPYA_1065. Requires tract_df to be provided.
        tract_list : Optional[Union[List[str], str]]
            Specific tracts to visualize. Can be:
            - List of tract names: ['AF_L', 'IFOF_L'] - filters tract_df or specifies tracts directly
            - String "all": uses all tracts (from abbreviations file or discovered from trk_dir)
            - None: uses all tracts in tract_df (requires tract_df)
            Required when tract_df is None (default: None)
        color_scheme : str
            Color scheme for visualization: 'coolwarm', 'viridis', 'magma', 'fds', etc. 
            Used with values_column (when tract_df provided) or automatically with tract_list (when no tract_df).
            Ignored if color_column or single_color is specified (default: 'coolwarm')
        color_column : Optional[str]
            Name of column in tract_df containing colors for each tract. Requires tract_df.
            Column should contain RGB tuples (e.g., (255, 0, 0)), hex colors (e.g., '#FF0000'), 
            or color names (e.g., 'red'). Takes priority over values_column (default: None)
        single_color : Optional[Union[str, Tuple[float, float, float], List[Union[str, Tuple[float, float, float]]]]]
            Color(s) to use for tracts. Takes priority over color_column and values_column.
            Can be specified as:
            - Single color for all tracts:
              * Hex color: '#FF0000', '#ff0000'
              * Color name: 'red', 'blue', 'green' (matplotlib color names)
              * RGB tuple: (1.0, 0.0, 0.0) for normalized values or (255, 0, 0) for 0-255 values
              * RGB string: '255 0 0'
            - List of colors (one per tract): ['red', '#FF0000', (0, 255, 0)]
              * Must match the length of tract_list or tracts in tract_df
              * Each element can use any of the single color formats above (default: None)
        plot_mode : str
            'iterative' (default): Plot one tract at a time
            'all_tracts': Plot all tracts together in a grid layout
        view : str or List[str]
            View type specification (default: 'lateral')
            - String: Single view applied to all tracts ('lateral' or 'medial')
            - List: Individual view for each tract (only supported with tract_gradient_plot=True)
            'lateral': Show lateral hemisphere view
            'medial': Show medial hemisphere view
        grid_orientation : Optional[str]
            Grid layout orientation for multi-view plots (default: None)
            None: Use single view specified by 'view' parameter
            'vertical': lateral views on top row, medial views on bottom row
            'horizontal': lateral views on left column, medial views on right column
            For single hemisphere: creates 1x2 (vertical) or 2x1 (horizontal) layout
            Can be used with both 'iterative' (per-tract grids) and 'all_tracts' modes
        tract_gradient_plot : bool
            Create a gradient plot of tracts with flexible grid layout (default: False)
            Requirements: Only works in 'iterative' mode with color_scheme, color_column, or values_column
            Behavior:
            - No tract_list + tract_df + values_column: Sort by values, select evenly spaced tracts
            - tract_list + tract_df + values_column: Sort specified tracts by values
            - tract_list only: Plot tracts in specified order
            Colors always based on full tract_df when available, not just plotted subset
            Grid layout automatically adjusts to accommodate the specified number of tracts
        gradient_n_tracts : int
            Maximum number of tracts to include in gradient plot (default: 10)
            Only used when tract_gradient_plot=True and tract_list is not provided
            If tract_list is specified, uses all tracts in the list regardless of this value
            For large numbers, creates multi-row grid with optimized column distribution
        colorbar : bool
            Add colorbar/legend below the visualization (default: False)
            - Continuous colorbar: when using color_scheme, values_column, or color_column
            - Discrete legend: when using single_color (shows colors with tract names from abbreviations or inferred short names)
            Works with all plot modes (iterative, all_tracts, tract_gradient_plot) and layouts
        colorbar_title : Optional[str]
            Custom title for the colorbar (default: None). If None, title is derived from values_column, color_column, or color_scheme.
        output_name : str
            Name for output files (default: 'tract_visualization')
        output_dir : Optional[str]
            Custom directory path for saving output images (default: None)
            If None, uses the default output directory set during initialization
            If specified, will create the directory if it doesn't exist
        keep_csv : bool
            Whether to keep CSV command files for debugging (default: False)
        keep_color_files : bool
            Whether to keep color text files after processing (default: False)
        cleanup_renamed_files : bool
            Whether to delete numbered tract files after visualization (default: True)
        include_bilateral : bool
            If True, look for tracts in trk_dir/bilateral and plot them on both left and right hemisphere views (default: True). Set False for older DSI Studio layouts that have only left_hem and right_hem.
        value_range_override : Optional[Tuple[float, float]]
            If provided, (vmin, vmax) for the colormap and colorbar instead of using data min/max. Use for fixed scale across plots.
            
        Examples:
        ---------
        # Basic usage with Gini coefficients
        tract_data = pd.DataFrame({
            'Tract_Short_Name': ['AF_L', 'IFOF_L', 'SLF_I_L'],
            'Gini_Coefficient': [0.8, 0.3, 0.6]
        })
        viz.visualize_tracts(tract_data, values_column='Gini_Coefficient')
        
        # Single color for all tracts
        tract_data = pd.DataFrame({
            'Tract_Short_Name': ['AF_L', 'IFOF_L', 'SLF_I_L']
        })
        viz.visualize_tracts(tract_data, single_color='red', plot_mode='all_tracts')
        
        # Subset of tracts with specific list
        large_tract_data = pd.DataFrame({
            'Tract_Short_Name': ['AF_L', 'IFOF_L', 'SLF_I_L', 'UF_L', 'ILF_L'],
            'Gini_Coefficient': [0.8, 0.3, 0.6, 0.4, 0.7]
        })
        viz.visualize_tracts(large_tract_data, values_column='Gini_Coefficient', 
                           tract_list=['AF_L', 'IFOF_L'])  # Only plot AF_L and IFOF_L
        
        # Plot specific tracts without DataFrame (single color)
        viz.visualize_tracts(tract_list=['AF_L', 'IFOF_L', 'UF_L'], single_color='blue')
        
        # Plot specific tracts without DataFrame (colormap)
        viz.visualize_tracts(tract_list=['AF_L', 'IFOF_L', 'UF_L'], color_scheme='viridis')
        
        # Plot all available tracts (from abbreviations file or trk_dir)
        viz.visualize_tracts(tract_list='all', single_color='red', plot_mode='all_tracts')
        
        # Plot all tracts with coolwarm colormap (no DataFrame needed)
        viz.visualize_tracts(tract_list='all', color_scheme='coolwarm', plot_mode='all_tracts')
        
        # List of colors for specific tracts (no DataFrame needed)
        viz.visualize_tracts(tract_list=['AF_L', 'IFOF_L', 'UF_L'], 
                           single_color=['red', '#FF0000', (0, 255, 0)])
        
        # Single medial view (iterative mode)
        viz.visualize_tracts(tract_list=['AF_L'], single_color='blue', view='medial')
        
        # Per-tract grids in iterative mode
        viz.visualize_tracts(tract_list=['AF_L', 'IFOF_L'], single_color='red',
                           grid_orientation='vertical')  # Each tract gets its own grid
        
        # Horizontal grid layout for all tracts
        viz.visualize_tracts(tract_list=['AF_L', 'AF_R'], single_color='blue', 
                           plot_mode='all_tracts', grid_orientation='horizontal')
        
        # Tract gradient plot (flexible grid layout)
        viz.visualize_tracts(df, values_column='Gini_Coefficient', 
                           tract_gradient_plot=True, gradient_n_tracts=15)  # Auto-selects evenly spaced tracts
        
        # Gradient plot with uniform lateral view
        viz.visualize_tracts(df, tract_list=['AF_L', 'IFOF_L', 'UF_L'], 
                           values_column='Gini_Coefficient', tract_gradient_plot=True, view='lateral')
        
        # Gradient plot with different views per tract (single-row grid)
        viz.visualize_tracts(df, tract_list=['AF_L', 'IFOF_L', 'UF_L'], 
                           values_column='Gini_Coefficient', tract_gradient_plot=True, 
                           view=['lateral', 'medial', 'lateral'])
        
        # Add colorbar to visualization
        viz.visualize_tracts(df, values_column='Gini_Coefficient', colorbar=True)
        
        # Discrete legend for single colors
        viz.visualize_tracts(tract_list=['AF_L', 'IFOF_L'], single_color=['red', 'blue'], 
                           colorbar=True)  # Shows tract names from abbreviations or inferred
        
        # Using color_column for per-tract colors
        tract_data = pd.DataFrame({
            'Tract_Short_Name': ['AF_L', 'IFOF_L', 'UF_L'],
            'Gini_Coefficient': [0.8, 0.3, 0.6],
            'colors': ['#FF0000', (0, 255, 0), 'blue']  # Mix of hex, RGB tuple, and color name
        })
        viz.visualize_tracts(tract_data, color_column='colors', plot_mode='all_tracts')
        
        viz.visualize_tracts(tract_data, values_column='Gini_Coefficient', plot_mode='all_tracts')
        """
        # Per-call override for bilateral tract inclusion (used by _find_tract_file and _discover_tracts_from_filesystem)
        self._include_bilateral = include_bilateral
        
        # Store original output directory and set custom one if provided
        original_output_dir = self.output_dir
        if output_dir is not None:
            self.output_dir = output_dir
            # Create custom output directory if it doesn't exist
            os.makedirs(self.output_dir, exist_ok=True)
            print(f"Using custom output directory: {self.output_dir}")
        
        # Validate inputs based on DataFrame presence
        has_dataframe = tract_df is not None
        
        # Check for DataFrame requirement cases
        requires_dataframe = (
            values_column is not None or 
            color_column is not None or
            tract_name_column != 'Tract_Short_Name' or
            (tract_list is None)
        )
        
        if not has_dataframe and requires_dataframe:
            if values_column is not None:
                raise ValueError("tract_df must be provided when values_column is specified")
            elif color_column is not None:
                raise ValueError("tract_df must be provided when color_column is specified")
            elif tract_name_column != 'Tract_Short_Name':
                raise ValueError("tract_df must be provided when custom tract_name_column is specified")
            elif tract_list is None:
                raise ValueError("Either tract_df or tract_list must be provided")
        
        if not has_dataframe and tract_list is None:
            raise ValueError("Either tract_df or tract_list must be provided")
        
        # Validate DataFrame columns if DataFrame is provided
        if has_dataframe:
            if tract_name_column not in tract_df.columns:
                raise ValueError(f"Column '{tract_name_column}' not found in tract_df. Available columns: {list(tract_df.columns)}")
            if values_column is not None and values_column not in tract_df.columns:
                raise ValueError(f"Column '{values_column}' not found in tract_df. Available columns: {list(tract_df.columns)}")
            if color_column is not None and color_column not in tract_df.columns:
                raise ValueError(f"Column '{color_column}' not found in tract_df. Available columns: {list(tract_df.columns)}")
        
        # Check for color specification priority: single_color > color_column > values_column > color_scheme
        has_single_color = single_color is not None
        has_color_column = has_dataframe and color_column is not None and color_column in tract_df.columns
        has_values_column = has_dataframe and values_column is not None and values_column in tract_df.columns
        has_color_scheme = color_scheme is not None  # Always true since it has a default, but for clarity
        
        if not has_single_color and not has_color_column and not has_values_column:
            if has_dataframe:
                raise ValueError("Must provide either 'single_color', 'color_column', or 'values_column' (with valid column name) in tract_df")
            elif not has_color_scheme:
                raise ValueError("Must provide either 'single_color' or 'color_scheme' when no tract_df is provided")
            # If we get here: no tract_df, but we have color_scheme - this is valid
        
        # Report color mode
        if has_single_color:
            if isinstance(single_color, list):
                print(f"Using list of {len(single_color)} colors for tracts")
            else:
                print(f"Using single color '{single_color}' for all tracts")
        elif has_color_column:
            print(f"Using colors from '{color_column}' column")
        elif has_values_column:
            print(f"Using values from '{values_column}' column with {color_scheme} colormap")
        else:
            print(f"Using {color_scheme} colormap for tract_list (no DataFrame provided)")
        
        # Get full dataset values and colors for proper colorbar range (BEFORE any filtering)
        full_dataset_values = None
        full_dataset_colors = None
        if has_dataframe and has_values_column and values_column is not None:
            full_dataset_values = tract_df[values_column].tolist()
            print(f"Full dataset range for colorbar: {min(full_dataset_values):.3f} to {max(full_dataset_values):.3f}")
            
        # Also get full dataset colors if color_column is specified
        if has_dataframe and has_color_column and color_column is not None:
            # Sort full dataset by values for consistent color mapping
            full_df_sorted = tract_df.sort_values(by=values_column, ascending=True) if has_values_column else tract_df
            full_dataset_colors = {}
            full_dataset_tract_names = []
            
            for _, row in full_df_sorted.iterrows():
                tract_name = row[tract_name_column]
                color_value = row[color_column]
                r, g, b = self._parse_color_value(color_value, tract_name)
                full_dataset_colors[tract_name] = (r, g, b, 1.0)
                full_dataset_tract_names.append(tract_name)
                
            print(f"Extracted colors from '{color_column}' column for {len(full_dataset_colors)} tracts (full dataset)")
        
        # Handle tract_list processing
        if tract_list is not None:
            # Handle special case: tract_list="all"
            if tract_list == "all":
                tract_list = self._get_all_tracts_from_abbreviations()
                print(f"Expanded tract_list='all' to {len(tract_list)} tracts")
            elif isinstance(tract_list, str) and tract_list != "all":
                # Convert single string to list to prevent character iteration
                tract_list = [tract_list]
                print(f"Converted single tract string '{tract_list[0]}' to list")
            
            if has_dataframe:
                # Filter DataFrame to specific tracts
                original_count = len(tract_df)
                tract_df_filtered = tract_df[tract_df[tract_name_column].isin(tract_list)].copy()
                
                if tract_df_filtered.empty:
                    raise ValueError(f"No tracts found in tract_df matching tract_list. Available tracts: {list(tract_df[tract_name_column].unique())}")
                
                missing_tracts = set(tract_list) - set(tract_df_filtered[tract_name_column])
                if missing_tracts:
                    print(f"Warning: The following tracts from tract_list were not found in tract_df: {list(missing_tracts)}")
                
                tract_df = tract_df_filtered
                print(f"Filtered DataFrame from {original_count} to {len(tract_df)} tracts based on tract_list")
                print(f"Selected tracts: {list(tract_df[tract_name_column])}")
            else:
                # No DataFrame - will use tract_list directly
                print(f"Using tract_list directly (no DataFrame): {len(tract_list)} tracts")
                print(f"Selected tracts: {tract_list[:10]}{'...' if len(tract_list) > 10 else ''}")
        
        # Process tract data based on DataFrame availability
        if has_dataframe:
            # Sort DataFrame by values column if we have values to sort by
            if has_values_column and not has_single_color:
                tract_df_sorted = tract_df.sort_values(by=values_column, ascending=True).reset_index(drop=True)
                print(f"Sorted {len(tract_df_sorted)} tracts by {values_column} (ascending)")
                print(f"Value range: {tract_df_sorted[values_column].min():.3f} to {tract_df_sorted[values_column].max():.3f}")
                sorted_values = tract_df_sorted[values_column].tolist()
            else:
                # No sorting needed for single color mode, use original order
                tract_df_sorted = tract_df.reset_index(drop=True)
                sorted_values = None
                if has_single_color:
                    print(f"Using original tract order for single color visualization")
                else:
                    print(f"Using original tract order (no values column for sorting)")
            
            # Extract tract names from DataFrame
            final_tract_list = tract_df_sorted[tract_name_column].tolist()
        else:
            # No DataFrame - use tract_list directly
            final_tract_list = tract_list
            sorted_values = None
            tract_df_sorted = None
            print(f"Using tract_list directly (no DataFrame sorting)")
        
        # Match tract names one by one to preserve the sorting order
        matched_tracts = []
        matched_values = []
        
        for i, original_tract in enumerate(final_tract_list):
            matched_for_tract = self._match_tract_names([original_tract])
            if matched_for_tract:
                matched_tracts.extend(matched_for_tract)  # May return multiple matches (L/R for abbreviations)
                # Repeat the value for each matched tract (if we have values)
                if sorted_values is not None:
                    for _ in matched_for_tract:
                        matched_values.append(sorted_values[i])
                else:
                    # No values - just add None placeholders or sequential indices
                    for _ in matched_for_tract:
                        matched_values.append(None)
        
        print(f"Matched {len(matched_tracts)} tracts for visualization")
        print(f"First 5 matched tracts with values: {[(t, v) for t, v in zip(matched_tracts[:5], matched_values[:5])]}")
        
        if not matched_tracts:
            print("Error: No valid tracts found to visualize")
            return
        
        # Create color dictionary based on priority: single_color > color_column > values_column > color_scheme
        rgb_color_dict = None
        
        if has_single_color:
            rgb_color_dict = {}
            
            # Check if single_color is a list of colors
            if isinstance(single_color, list):
                # Validate list length matches number of tracts
                if len(single_color) != len(final_tract_list):
                    raise ValueError(f"single_color list length ({len(single_color)}) must match tract count ({len(final_tract_list)}). "
                                   f"Tract list: {final_tract_list}")
                
                # Apply colors from list to matched tracts in order
                for i, original_name in enumerate(final_tract_list):
                    matched_for_this = self._match_tract_names([original_name])
                    color_value = single_color[i]
                    
                    # Parse the color value
                    r, g, b = self._parse_color_value(color_value, "single_color")
                    
                    # Assign the same RGB color to all matched tracts for this original tract
                    for matched_name in matched_for_this:
                        rgb_color_dict[matched_name] = (r, g, b, 1.0)
        
                print(f"Applied {len(single_color)} colors from single_color list to {len(matched_tracts)} tracts")
            else:
                # Single color for all tracts (original behavior)
                single_color_rgb = self._parse_color_value(single_color, "single_color")
                for matched_name in matched_tracts:
                    rgb_color_dict[matched_name] = (*single_color_rgb, 1.0)  # Add alpha channel
                print(f"Applied single color {single_color_rgb} to {len(matched_tracts)} tracts")
        
        elif has_color_column:
            rgb_color_dict = {}
            
            # Create RGB color dictionary using the color_column (requires DataFrame)
            for i, original_name in enumerate(final_tract_list):
                matched_for_this = self._match_tract_names([original_name])
                color_value = tract_df_sorted.iloc[i][color_column]
                
                # Parse the color value (could be RGB tuple, hex string, or color name)
                r, g, b = self._parse_color_value(color_value, f"tract_{original_name}")
                
                # Assign the same RGB color to all matched tracts for this original tract
                for matched_name in matched_for_this:
                    rgb_color_dict[matched_name] = (r, g, b, 1.0)
            
            print(f"Applied colors from '{color_column}' column to {len(matched_tracts)} tracts")
        
        elif has_values_column:
            # This case is handled by the colormap in _create_color_file method
            # rgb_color_dict remains None to signal colormap usage
            pass
        
        elif not has_dataframe and has_color_scheme:
            # Use color_scheme to automatically assign colors to tract_list (no DataFrame)
            rgb_color_dict = {}
            
            # Generate colors from colormap for the tract list
            # Normalize tract indices to [0, 1] range for colormap
            num_tracts = len(matched_tracts)
            if num_tracts == 1:
                # Single tract gets the middle of the colormap
                colormap_values = [0.5]
            else:
                # Multiple tracts get evenly distributed colors
                colormap_values = [i / (num_tracts - 1) for i in range(num_tracts)]
            
            # Select colormap
            if color_scheme == 'fds' and HAS_TM_UTILS:
                cmap = fds_cmap
            else:
                cmap = plt.cm.get_cmap(color_scheme)
            
            # Apply colors to matched tracts
            for i, matched_name in enumerate(matched_tracts):
                rgb = cmap(colormap_values[i])[:3]  # Get RGB, ignore alpha
                rgb_color_dict[matched_name] = (*rgb, 1.0)  # Add alpha channel
            
            print(f"Applied {color_scheme} colormap to {len(matched_tracts)} tracts from tract_list")
        
        # Validate plot_mode and grid_orientation
        if plot_mode not in ['iterative', 'all_tracts']:
            raise ValueError(f"plot_mode must be 'iterative' or 'all_tracts', got '{plot_mode}'")
        
        # Validate view parameter - allow lists for tract_gradient_plot mode
        if tract_gradient_plot:
            # For tract_gradient_plot, view can be string or list - validation happens later in _visualize_tract_gradient_plot
            if isinstance(view, str):
                if view not in ['lateral', 'medial']:
                    raise ValueError(f"view must be 'lateral' or 'medial', got '{view}'")
            elif isinstance(view, list):
                for i, v in enumerate(view):
                    if v not in ['lateral', 'medial']:
                        raise ValueError(f"All view elements must be 'lateral' or 'medial', got '{v}' at position {i}")
            else:
                raise ValueError(f"view must be a string or list of strings when tract_gradient_plot=True, got {type(view)}")
        else:
            # For other modes, view must be a single string
            if not isinstance(view, str) or view not in ['lateral', 'medial']:
                raise ValueError(f"view must be 'lateral' or 'medial' (string), got '{view}'. Lists are only supported with tract_gradient_plot=True")
        
        if grid_orientation is not None and grid_orientation not in ['vertical', 'horizontal']:
            raise ValueError(f"grid_orientation must be None, 'vertical' or 'horizontal', got '{grid_orientation}'")
        
        # Validate tract_gradient_plot requirements
        if tract_gradient_plot:
            if plot_mode != 'iterative':
                raise ValueError("tract_gradient_plot can only be used with plot_mode='iterative'")
            
            if not (has_single_color or has_color_scheme or has_color_column or has_values_column):
                raise ValueError("tract_gradient_plot requires single_color, color_scheme, color_column, or values_column to be provided")
            
            if grid_orientation is not None:
                raise ValueError("tract_gradient_plot cannot be combined with grid_orientation (creates its own single-row grid)")
        
        # Convert matched_values to None if all values are None (single_color mode)
        final_values = matched_values if any(v is not None for v in matched_values) else None
        
        # Global value range for colormap: same metric value -> same color across views/hemispheres
        value_range = None
        if value_range_override is not None:
            value_range = value_range_override
            print(f"Using override value range for colormap: {value_range[0]:.3f} to {value_range[1]:.3f}")
        elif has_values_column and (full_dataset_values or final_values):
            src = full_dataset_values if full_dataset_values else final_values
            if src and all(v is not None for v in src):
                vmin, vmax = min(src), max(src)
                if vmax > vmin:
                    value_range = (vmin, vmax)
                    print(f"Using global value range for colormap: {vmin:.6f} to {vmax:.6f}")
        
        if plot_mode == 'all_tracts':
            self._visualize_all_tracts(matched_tracts, color_scheme, final_values, 
                                     rgb_color_dict, output_name, grid_orientation, colorbar,
                                     has_single_color, has_values_column, values_column,
                                     has_color_column, color_column, full_dataset_values,
                                     full_dataset_colors, keep_csv, keep_color_files, cleanup_renamed_files, view,
                                     value_range, colorbar_title)
        elif tract_gradient_plot:
            self._visualize_tract_gradient_plot(tract_df, matched_tracts, color_scheme, final_values,
                                              rgb_color_dict, output_name, tract_name_column, tract_list,
                                              has_values_column, values_column, colorbar, view, gradient_n_tracts,
                                              has_single_color, has_color_column, color_column,
                                              full_dataset_values, full_dataset_colors, keep_csv, keep_color_files, cleanup_renamed_files,
                                              value_range, colorbar_title)
        else:
            self._visualize_tracts_iterative(matched_tracts, color_scheme, final_values,
                                           rgb_color_dict, output_name, view, grid_orientation, colorbar,
                                           has_single_color, has_values_column, values_column,
                                           has_color_column, color_column, full_dataset_values,
                                           full_dataset_colors, keep_csv, keep_color_files, cleanup_renamed_files,
                                           value_range, colorbar_title)
        
        # Restore original output directory and clear per-call bilateral override
        if hasattr(self, '_include_bilateral'):
            del self._include_bilateral
        self.output_dir = original_output_dir

    # ------------------------------------------------------------
    # Tract name matching and color dictionary conversion
    # ------------------------------------------------------------

    def _load_abbreviations(self) -> pd.DataFrame:
        """Load tract abbreviations from Excel or CSV file (path set via abbreviations_file in __init__)."""
        if not os.path.exists(self.abbreviations_file):
            print(f"Warning: Abbreviations file not found at {self.abbreviations_file}. Will infer tract names from filenames in trk_dir.")
            return pd.DataFrame()
        path_lower = self.abbreviations_file.lower()
        if path_lower.endswith('.csv'):
            return pd.read_csv(self.abbreviations_file)
        return pd.read_excel(self.abbreviations_file)

    def _discover_tracts_from_filesystem(self) -> List[str]:
        """Discover tract names from filenames in trk_dir/left_hem, right_hem, and optionally bilateral. Cached after first call."""
        if self._discovered_tracts is not None:
            return self._discovered_tracts
        tracts = []
        subdirs = ['left_hem', 'right_hem']
        if getattr(self, '_include_bilateral', True):
            subdirs.append('bilateral')
        for subdir in subdirs:
            dirpath = os.path.join(self.trk_dir, subdir)
            if not os.path.isdir(dirpath):
                continue
            for f in os.listdir(dirpath):
                for ext in self.TRACT_EXTENSIONS:
                    if f.endswith(ext):
                        stem = f[:-len(ext)]
                        if stem and stem not in tracts:
                            tracts.append(stem)
                        break
        self._discovered_tracts = sorted(tracts)
        if self._discovered_tracts:
            print(f"Discovered {len(self._discovered_tracts)} tracts from {self.trk_dir} (no abbreviations file)")
        return self._discovered_tracts

    def _canonical_to_short_name(self, canonical_name: str) -> str:
        """
        Derive a short display name from a canonical tract name (filename stem).
        E.g. ICBM152_adult.Association_InferiorLongitudinalFasciculusL -> ILF_L.
        If the name is already short (few segments or short length), return it with hemisphere.
        """
        if not canonical_name:
            return canonical_name
        hem = ''
        name = canonical_name
        # Strip hemisphere suffix: _L/_R, .L/.R, or trailing L/R (e.g. InferiorLongitudinalFasciculusL)
        if name.endswith('_L') or name.endswith('_R'):
            hem = '_' + name[-1]
            name = name[:-2].rstrip('_.')
        elif name.endswith('.L') or name.endswith('.R'):
            hem = '_' + name[-1]
            name = name[:-2].rstrip('_.')
        else:
            parts_pre = re.split(r'_+|(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])', name)
            parts_pre = [p for p in parts_pre if p]
            if parts_pre and parts_pre[-1] in ('L', 'R'):
                hem = '_' + parts_pre[-1]
                name = name[:-len(parts_pre[-1])].rstrip('_.')
        # Take part after last dot (e.g. Category_TractName)
        if '.' in name:
            name = name.split('.')[-1]
        # Ignore category prefix: build acronym only from the part after the first underscore
        # (e.g. Association_InferiorLongitudinalFasciculus -> InferiorLongitudinalFasciculus)
        if '_' in name:
            name = name.split('_', 1)[1]
        # Already short? (e.g. AF, SLF)
        if len(name) <= 12 and '_' not in name:
            return name + hem if hem else name
        # Split by underscore and CamelCase, take first letter of each segment for acronym
        parts = re.split(r'_+|(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])', name)
        parts = [p for p in parts if p]
        if not parts:
            return canonical_name
        acronym = ''.join(p[0].upper() for p in parts if p)
        return acronym + hem if hem else acronym

    def _match_tract_names(self, tract_list: List[str], 
                         tract_color_dict: Optional[Dict[str, tuple]] = None) -> Union[List[str], Tuple[List[str], Dict[str, tuple]]]:
        """
        Match tract identifiers to canonical tract names (filename stems in trk_dir).
        When abbreviations file is loaded: supports Tract_Long_Name, Tract, Abbreviation,
        new_qsirecon_tract_names, HCPYA_1065. When no abbreviations: exact match on
        discovered filenames or match by inferred short name (e.g. "ILF" -> ILF_L, ILF_R).
        If tract_color_dict is provided, converts its keys to canonical names.
        """
        if not self.abbreviations.empty:
            return self._match_tract_names_via_abbreviations(tract_list, tract_color_dict)
        # No abbreviations: use filesystem-discovered tracts
        discovered = self._discover_tracts_from_filesystem()
        if not discovered:
            print("Warning: No tracts discovered in trk_dir and no abbreviations file loaded")
            return ([], {}) if tract_color_dict is not None else []
        matched_tracts = []
        for tract in tract_list:
            if tract == "all":
                matched_tracts.extend(discovered)
                print(f"Matched 'all' -> {len(discovered)} tracts from trk_dir")
                continue
            if tract in discovered:
                matched_tracts.append(tract)
                print(f"Matched '{tract}' -> {tract}")
                continue
            # Match by short name (e.g. "ILF" -> tracts with short name ILF_L, ILF_R)
            short_upper = tract.replace(' ', '_').strip('_').upper()
            found_any = False
            for c in discovered:
                short = self._canonical_to_short_name(c)
                short_base = short.rsplit('_', 1)[0] if '_' in short and short.endswith(('_L', '_R')) else short
                if short == tract or short_base == short_upper or short.upper() == short_upper:
                    matched_tracts.append(c)
                    print(f"Matched '{tract}' -> {c} (short: {short})")
                    found_any = True
            if not found_any:
                print(f"Warning: No match found for '{tract}' in discovered tracts")
        matched_tracts = list(dict.fromkeys(matched_tracts))  # preserve order, remove dupes
        if tract_color_dict is None:
            return matched_tracts
        converted_color_dict = {}
        for original_key, color_tuple in tract_color_dict.items():
            if original_key in discovered:
                converted_color_dict[original_key] = color_tuple
            else:
                for c in discovered:
                    if self._canonical_to_short_name(c) == original_key or c == original_key:
                        converted_color_dict[c] = color_tuple
                        break
                else:
                    converted_color_dict[original_key] = color_tuple
        return matched_tracts, converted_color_dict

    def _match_tract_names_via_abbreviations(self, tract_list: List[str],
                         tract_color_dict: Optional[Dict[str, tuple]] = None) -> Union[List[str], Tuple[List[str], Dict[str, tuple]]]:
        """Match tract names using abbreviations file (Tract_Long_Name, Tract, Abbreviation, etc.)."""
        matched_tracts = []
        for tract in tract_list:
            matches = self.abbreviations[
                (self.abbreviations['Tract_Long_Name'] == tract) |
                (self.abbreviations['Tract'] == tract) |
                (self.abbreviations['Abbreviation'] == tract) |
                (self.abbreviations['new_qsirecon_tract_names'] == tract) |
                (self.abbreviations['HCPYA_1065'] == tract)
            ]
            if not matches.empty:
                hcpya_names = matches['HCPYA_1065'].dropna().unique()
                matched_tracts.extend(hcpya_names)
                print(f"Matched '{tract}' -> {list(hcpya_names)}")
            else:
                print(f"Warning: No match found for '{tract}' in abbreviations file")
        matched_tracts = list(set(matched_tracts))
        if tract_color_dict is None:
            return matched_tracts
        converted_color_dict = {}
        for original_key, color_tuple in tract_color_dict.items():
            matches = self.abbreviations[
                (self.abbreviations['Tract_Long_Name'] == original_key) |
                (self.abbreviations['Tract'] == original_key) |
                (self.abbreviations['Abbreviation'] == original_key) |
                (self.abbreviations['new_qsirecon_tract_names'] == original_key) |
                (self.abbreviations['HCPYA_1065'] == original_key)
            ]
            if not matches.empty:
                for hcpya_name in matches['HCPYA_1065'].dropna().unique():
                    converted_color_dict[hcpya_name] = color_tuple
            else:
                converted_color_dict[original_key] = color_tuple
        return matched_tracts, converted_color_dict
    
    def _get_all_tracts_from_abbreviations(self) -> List[str]:
        """Get all available tract names (from abbreviations file or from trk_dir when no file)."""
        if not self.abbreviations.empty:
            all_tracts = self.abbreviations['HCPYA_1065'].dropna().unique().tolist()
            print(f"Found {len(all_tracts)} tracts from abbreviations file")
            return all_tracts
        all_tracts = self._discover_tracts_from_filesystem()
        if not all_tracts:
            raise ValueError("No tracts found. Provide an abbreviations file or ensure trk_dir contains left_hem, right_hem, and/or bilateral with tract files.")
        return all_tracts

    # ------------------------------------------------------------
    # Color processing
    # ------------------------------------------------------------
        
    
    def _create_color_file(self, tract_list: List[str], output_path: str,
                         color_scheme: str = 'cool_warm',
                         values: Optional[List[float]] = None,
                         tract_color_dict: Optional[Dict[str, tuple]] = None,
                         value_range: Optional[Tuple[float, float]] = None) -> None:
        """
        Internal method to create RGB color file for DSI Studio directly from tract data.
        Colors are ordered to match the numbered tract files generated by the _create_numbered_tract_files method.
        
        Parameters:
        -----------
        tract_list : List[str]
            List of tract names
        output_path : str
            Path to save the RGB color file
        color_scheme : str
            Color scheme name ('coolwarm', 'viridis', 'plasma', 'fds', etc.)
        values : Optional[List[float]]
            Values along which color mapping (e.g., Gini coefficients) is performed. Note that sorting is not performed here, but is done in the _create_numbered_tract_files method.
        tract_color_dict : Optional[Dict[str, tuple]]
            Dictionary mapping tract names to RGBA color tuples. Takes priority over values/color_scheme.
        value_range : Optional[Tuple[float, float]]
            (vmin, vmax) for normalizing values to [0,1]. If provided, the same range is used so the same metric value maps to the same color across hemispheres/calls. If None, min/max of the passed values is used.
        """
            
        # Handle tract color dictionary case
        if tract_color_dict:
            # No need to sort - colors will match numbered file order
            with open(output_path, 'w') as f:
                for tract in tract_list:
                    if tract in tract_color_dict:
                        color_tuple = tract_color_dict[tract]
                        rgb = color_tuple[:3]  # Get RGB, ignore alpha
                        rgb_str = f"{int(rgb[0]*255)} {int(rgb[1]*255)} {int(rgb[2]*255)}"
                        f.write(f"{rgb_str}\n")
                    else:
                        # Fallback to default color if tract not in dictionary
                        print(f"Warning: Tract '{tract}' not found in color dictionary, using default color")
                        rgb_str = "128 128 128"  # Gray fallback
                        f.write(f"{rgb_str}\n")
            print(f"Created color file using tract color dictionary: {output_path}")
            return
        
        # Set default values if none provided
        if values is None:
            values = np.linspace(0, 1, len(tract_list))
        
        # Normalize values to [0, 1] using global range when provided, else min/max of passed values.
        # Using a global range ensures the same metric value maps to the same color across hemispheres and gradient plots.
        values = np.asarray(values, dtype=float)
        if value_range is not None:
            vmin, vmax = value_range[0], value_range[1]
        else:
            vmin, vmax = float(np.min(values)), float(np.max(values))
        if vmax > vmin:
            values_norm = np.clip((values - vmin) / (vmax - vmin), 0.0, 1.0)
        else:
            values_norm = np.full_like(values, 0.5)  # all same value -> middle of colormap
        
        # Select colormap
        if color_scheme == 'fds' and HAS_TM_UTILS:
            cmap = fds_cmap
        else:
            cmap = plt.cm.get_cmap(color_scheme)
        
        # Generate RGB colors directly and write to file in original order
        # The numbered tract files will control the loading order of tracts in DSI Studio (so no need to sort here)
        with open(output_path, 'w') as f:
            for val in values_norm:
                rgb = cmap(val)[:3]  # Get RGB, ignore alpha
                rgb_str = f"{int(rgb[0]*255)} {int(rgb[1]*255)} {int(rgb[2]*255)}"
                f.write(f"{rgb_str}\n")
                
        print(f"Created color file with {color_scheme} scheme: {output_path}")

    def _parse_color_value(self, color_value, context: str = "") -> Tuple[float, float, float]:
        """
        Parse color value from various formats into normalized (0-1) RGB tuple.
        
        Supports multiple input formats:
        - Hex colors: "#FF0000", "#ff0000"
        - Named colors: "red", "blue", "green" (matplotlib names)
        - RGB strings: "255 0 0", "1.0 0.0 0.0"
        - RGB tuples/lists: (255, 0, 0), [1.0, 0.0, 0.0]
        
        Parameters:
        -----------
        color_value : str, tuple, or list
            Color value in any supported format
        context : str, optional
            Context string for error messages (e.g., tract name)
            
        Returns:
        --------
        Tuple[float, float, float]
            Normalized (0-1) RGB tuple
        """
        if isinstance(color_value, str):
            # Handle hex colors (e.g., "#FF0000")
            if color_value.startswith('#'):
                try:
                    hex_color = color_value.lstrip('#')
                    if len(hex_color) == 6: 
                        return tuple(int(hex_color[i:i+2], 16)/255.0 for i in (0, 2, 4))
                except ValueError:
                    pass
            
            # Handle named colors using matplotlib (e.g., "red", "blue", "green")
            try:
                rgb = mcolors.to_rgb(color_value)
                return rgb
            except ValueError:
                pass
            
            # Handle space-separated RGB string (e.g., "255 0 0")
            rgb_parts = color_value.strip().split()
            if len(rgb_parts) >= 3:
                try:
                    return tuple(float(x)/255 for x in rgb_parts[:3])
                except ValueError:
                    pass
        
        elif isinstance(color_value, (tuple, list)) and len(color_value) >= 3:
            # Handle RGB tuple/list - normalize if needed
            try:
                rgb = [float(x) for x in color_value[:3]]
                # Check if values are in 0-255 range (need to normalize) or 0-1 range
                return tuple(x/255 if max(rgb) > 1.0 else x for x in rgb)
            except (ValueError, TypeError):
                pass
        
        # Default fallback - use red for single colors, gray for RGB parsing
        default_color = (1.0, 0.0, 0.0) if not context or "single_color" in context else (0.5, 0.5, 0.5)
        if context:
            print(f"Warning: Could not parse color '{color_value}' for {context}, using default")
        return default_color

    def _create_numbered_tract_files(self, tract_list: List[str], values: Optional[List[float]] = None,
                                   tract_color_dict: Optional[Dict[str, tuple]] = None) -> Tuple[List[str], str]:
        """
        Create numbered copies of tract trk files to ensure proper DSI Studio loading order. If this numbering isn't done, the order in which tracts are loaded in DSI Studio is not guaranteed to be the same as the order in the tract_list and therefore the coloring mapping would be incorrect.
        The tract_list should already be sorted by the calling function (dataframe approach). These copies are used to ensure the correct loading order of tracts in DSI Studio and are temporary; they are deleted after the visualization is created.
        
        Parameters:
        -----------
        tract_list : List[str]
            List of tract names (HCPYA_1065 format) in desired order
        values : Optional[List[float]]
            Values corresponding to tract order (for logging only)
        tract_color_dict : Optional[Dict[str, tuple]]
            Color dictionary (already converted to HCPYA_1065 keys)
            
        Returns:
        --------
        Tuple[List[str], str]
            (numbered_tract_files, numbered_dir)
        """
        # Create temporary directory for numbered files
        numbered_dir = f'{self.output_dir}/numbered_tract_files'
        os.makedirs(numbered_dir, exist_ok=True)
        
        # Use the tract_list as-is since it's already sorted by the DataFrame approach
        # No additional sorting needed here
        ordered_tracts = tract_list
        if values is not None:
            print(f"Using pre-sorted tract order with values: {[f'{tract}({val:.3f})' for tract, val in zip(tract_list, values)]}")
        else:
            print(f"Using pre-sorted tract order: {ordered_tracts}")
        
        # Find original tract files and create numbered copies
        numbered_tract_files = []
        missing_tracts = []
        
        for i, tract in enumerate(ordered_tracts):
            # Create 4-digit numbered prefix (like R script)
            rank = f"{i+1:04d}"
            
            # Find original tract file (.trk.gz or .tt.gz)
            original_file, tract_filename, subfolder = self._find_tract_file(tract)
            if original_file is None:
                missing_tracts.append(tract)
                continue
            
            # Create numbered filename (preserve original extension)
            numbered_filename = f"{rank}_{tract_filename}"
            numbered_filepath = os.path.join(numbered_dir, numbered_filename)
            
            # Copy file with numbered name
            shutil.copy2(original_file, numbered_filepath)
            numbered_tract_files.append(numbered_filepath)
            
            print(f"Created: {numbered_filename} (from {tract}, {subfolder})")
        
        if missing_tracts:
            print(f"Warning: Could not find files for tracts: {missing_tracts}")
            
        return numbered_tract_files, numbered_dir

    def _find_tract_file(self, tract: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Find a tract file in left_hem, right_hem, and optionally bilateral, trying .trk.gz and .tt.gz extensions.
        Returns (full_path, filename_with_ext, subfolder) or (None, None, None) if not found.
        Bilateral tracts are plotted on both left and right hemisphere views when include_bilateral is True.
        """
        subfolders = ['left_hem', 'right_hem']
        if getattr(self, '_include_bilateral', True):
            subfolders.append('bilateral')
        for ext in self.TRACT_EXTENSIONS:
            tract_filename = f'{tract}{ext}'
            for subfolder in subfolders:
                path = os.path.join(self.trk_dir, subfolder, tract_filename)
                if os.path.exists(path):
                    return path, tract_filename, subfolder
        return None, None, None

    def _tract_basename_matches(self, basename: str, tract: str) -> bool:
        """Return True if basename corresponds to the given tract (any supported extension)."""
        for ext in self.TRACT_EXTENSIONS:
            if basename.endswith(f'_{tract}{ext}') or basename == f'{tract}{ext}':
                return True
        return False

    # ------------------------------------------------------------
    # DSI Studio interface
    # ------------------------------------------------------------
  
    def _get_hemisphere_settings(self, tract_files: Union[str, List[str]], view_type: str = 'lateral') -> Tuple[str, str, bool]:
        """
        Get hemisphere-specific camera and surface settings.
        
        Parameters:
        -----------
        tract_files : Union[str, List[str]]
            List of tract files
        view_type : str
            View type: 'lateral' or 'medial'
            
        Returns:
        --------
        Tuple[str, str, bool]
            (surface_cmd, camera_cmd, is_right_hemisphere, zoom)
        """
        # Convert single file to list for consistent processing
        if isinstance(tract_files, str):
            tract_files = [tract_files]
            
        # Detect hemisphere: path contains right_hem, or filename has _R. (old), or ends with R.tt.gz/R.trk.gz (new, no underscore)
        def _is_right_tract(path_or_name: str) -> bool:
            base = os.path.basename(path_or_name)
            if 'right_hem' in path_or_name:
                return True
            if '_R.' in path_or_name and '_L.' not in path_or_name:
                return True
            if base.endswith('R.tt.gz') or base.endswith('R.trk.gz'):
                return True
            return False
        is_right_hemisphere = any(_is_right_tract(f) for f in tract_files)
        
        # Set surface and camera based on hemisphere and view type
        # ICBM152 template: use tuned camera/zoom (left lateral from user; rest inferred from HCP mirror pattern)
        if 'ICBM152' in self.fib_file:
            if is_right_hemisphere:
                surface_cmd = "add_surface_right,0,25"
                if view_type == 'lateral':
                    camera_cmd = "set_camera,0 0 1 0 -1 0 0 0 0 -1 0 0 49.5 39.5 -39.5 1,"
                    zoom = 1.1
                else:  # medial
                    camera_cmd = "set_camera,0 0 -1 0 1 0 0 0 0 -1 0 0 -49.5 39.5 39.5 1,"
                    zoom = 1.15
            else:
                surface_cmd = "add_surface_left,0,25"
                if view_type == 'lateral':
                    camera_cmd = "set_camera,0 0 -1 0 1 0 0 0 0 -1 0 0 -49.5 39.5 39.5 1,"
                    zoom = 1.1
                else:  # medial
                    camera_cmd = "set_camera,0 0 1 0 -1 0 0 0 0 -1 0 0 49.5 39.5 -39.5 1,"
                    zoom = 1.15
        else:
            # HCP1065 / legacy template
            if is_right_hemisphere:
                surface_cmd = "add_surface_right,0,25"
                if view_type == 'lateral':
                    camera_cmd = "set_camera,0 0 0.6 0 -0.6 0 0 0 0 -0.6 0 0 56.4 40.5 -46.8 1,"
                    zoom = 0.6
                else:  # medial
                    zoom = 0.65
                    camera_cmd = "set_camera,0 0 -0.65 0 0.65 0 0 0 0 -0.65 0 0 -61.1 43.875 50.7 1,"
            else:
                surface_cmd = "add_surface_left,0,25"
                if view_type == 'lateral':
                    camera_cmd = "set_camera,0 0 -0.6 0 0.6 0 0 0 0 -0.6 0 0 -56.4 40.5 46.8 1,"
                    zoom = 0.6
                else:  # medial
                    camera_cmd = "set_camera,0 0 0.65 0 -0.65 0 0 0 0 -0.65 0 0 61.1 43.875 -50.7 1,"
                    zoom = 0.65
        return surface_cmd, camera_cmd, is_right_hemisphere, zoom
    
    def _create_dsi_command_csv(self, csv_file: str, tract_files: Union[str, List[str]], 
                              color_file: str, output_image: str, view_type: str = 'lateral'):
        """
        Create a CSV command file for DSI Studio.
        Handles both single tract and multiple tract visualization.
        """
        # Convert single file to list for consistent processing
        if isinstance(tract_files, str):
            tract_files = [tract_files]
        
        surface_cmd, camera_cmd, is_right, zoom = self._get_hemisphere_settings(tract_files, view_type)
        
        # Build commands
        commands = [
            f"open_fib,{self.fib_file},",
            surface_cmd
        ]
        
        # Add tract files
        for tract_file in tract_files:
            commands.append(f"open_tract,{tract_file},")
        
        # Add remaining commands
        commands.extend([
            f"load_cluster_color,{color_file},",
            camera_cmd,
            f"set_zoom,{zoom},",
            f"save_hd_screen,{output_image},1024 800"
        ])
        
        # Write to file
        with open(csv_file, 'w') as f:
            for command in commands:
                f.write(command + '\n')
                
        hemisphere_str = "right" if is_right else "left"
        tract_count = len(tract_files)
        print(f"Created DSI Studio {view_type} view CSV for {tract_count} tract(s) ({hemisphere_str} hemisphere): {csv_file}")
 
    def _run_dsi_command(self, tract_label: str, csv_command_file: str, output_image: str, 
                        color_file: str, keep_csv: bool, keep_color_files: bool) -> None:
        """Run a DSI Studio command and handle cleanup."""
        
        cmd = f"{self.dsi_studio_path} {csv_command_file}"
        print(f"Running DSI Studio command for {tract_label}: {csv_command_file}")
        
        try:
            # Run with a small timeout to make sure it has time to load the tracts
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=8)
            if result.returncode == 0:
                print(f"Successfully created visualization for {tract_label}: {output_image}")
                
                # Ensure output image is exactly 1024x800 (DSI Studio sometimes outputs larger)
                self._ensure_image_dimensions(output_image, target_width=1024, target_height=800)
                
            else:
                print(f"DSI Studio error for {tract_label}: {result.stderr}")
                print(f"DSI Studio stdout for {tract_label}: {result.stdout}")
                
            # Clean up temporary CSV file unless keep_csv is True
            if not keep_csv and os.path.exists(csv_command_file):
                os.remove(csv_command_file)
            elif keep_csv and os.path.exists(csv_command_file):
                print(f"Kept CSV command file for debugging: {csv_command_file}")
            
        except subprocess.TimeoutExpired:
            print(f"DSI Studio command timed out for {tract_label}")
        except Exception as e:
            print(f"Error running DSI Studio for {tract_label}: {e}")
        finally:
            # cleanup csv file even if error occurred (unless keep_csv is True)
            if not keep_csv and os.path.exists(csv_command_file):
                os.remove(csv_command_file)


    def _ensure_image_dimensions(self, image_path: str, target_width: int = 1024, target_height: int = 800) -> None:
        """
        Ensure image is exactly the target dimensions.
        If larger, resize it. If smaller or exact, leave it unchanged. This is necessary because DSI Studio sometimes outputs images that are larger than the specified target dimensions, which would make a grid of brains look weird (varying sizes).
        
        Parameters:
        -----------
        image_path : str
            Path to the image file to check/resize
        target_width : int
            Target width in pixels (default: 1024)
        target_height : int
            Target height in pixels (default: 800)
        """
        if not os.path.exists(image_path):
            print(f"Warning: Image not found for dimension check: {image_path}")
            return
        
        print(f"Checking image dimensions: {os.path.basename(image_path)}")
        
        try:
            from PIL import Image
            
            # Load image and check dimensions
            img = Image.open(image_path)
            current_width, current_height = img.size
            
            if current_width == target_width and current_height == target_height:
                # Perfect size - no action needed
                img.close()
                return
            elif current_width > target_width or current_height > target_height:
                # Image is larger - resize it down
                resized_img = img.resize((target_width, target_height), Image.Resampling.LANCZOS)
                img.close()  # Close original before overwriting
                resized_img.save(image_path, quality=95)
                resized_img.close()
                print(f"Resized image from {current_width}x{current_height} to {target_width}x{target_height}: {os.path.basename(image_path)}")
            else:
                # Image is smaller - leave it as is (DSI Studio should handle this correctly)
                img.close()
                print(f"Image dimensions {current_width}x{current_height} are smaller than target {target_width}x{target_height}, keeping as-is: {os.path.basename(image_path)}")
                    
        except Exception as e:
            print(f"Error checking/resizing image dimensions for {image_path}: {e}")
            import traceback
            traceback.print_exc()
        
        # Final verification - check the actual dimensions after processing
        try:
            from PIL import Image
            with Image.open(image_path) as final_img:
                final_width, final_height = final_img.size
                print(f"Final image dimensions: {final_width}x{final_height} for {os.path.basename(image_path)}")
        except Exception as e:
            print(f"Could not verify final dimensions for {image_path}: {e}")

    def _get_tract_abbreviation(self, tract_name: str) -> str:
        """
        Get tract abbreviation and hemisphere (from abbreviations file or inferred from filename).
        
        Parameters:
        -----------
        tract_name : str
            Canonical tract name (e.g., filename stem or 'AF_L')
            
        Returns:
        --------
        str
            Display label (e.g., 'Left AF' or 'ILF L') or the original name if not found
        """
        try:
            if self.abbreviations.empty:
                short = self._canonical_to_short_name(tract_name)
                if '_L' in short:
                    return f"Left {short.replace('_L', '').strip()}"
                if '_R' in short:
                    return f"Right {short.replace('_R', '').strip()}"
                return short.replace('_', ' ')
            abbrev_df = self.abbreviations
            # Try exact match first
            exact_match = abbrev_df[abbrev_df['HCPYA_1065'] == tract_name]
            if not exact_match.empty and 'Abbreviation' in exact_match.columns:
                abbrev = exact_match.iloc[0]['Abbreviation']
                # Remove underscores and replace with spaces
                abbrev = abbrev.replace('_', ' ') if abbrev else abbrev
                # Add hemisphere if present in tract name
                if '_L' in tract_name:
                    return f"Left {abbrev}"
                elif '_R' in tract_name:
                    return f"Right {abbrev}"
                else:
                    return abbrev
            
            # If no exact match, try matching without hemisphere
            base_name = tract_name.replace('_L', '').replace('_R', '')
            partial_matches = abbrev_df[abbrev_df['HCPYA_1065'].str.contains(base_name, na=False)]
            if not partial_matches.empty and 'Abbreviation' in partial_matches.columns:
                abbrev = partial_matches.iloc[0]['Abbreviation']
                # Remove underscores and replace with spaces
                abbrev = abbrev.replace('_', ' ') if abbrev else abbrev
                # Add hemisphere if present in tract name
                if '_L' in tract_name:
                    return f"Left {abbrev}"
                elif '_R' in tract_name:
                    return f"Right {abbrev}"
                else:
                    return abbrev
            
            # If still no match found, return the original name with underscores replaced
            return tract_name.replace('_', ' ')
            
        except Exception as e:
            print(f"Warning: Could not get abbreviation for {tract_name}: {e}")
            return tract_name.replace('_', ' ')

    def _add_text_overlay(self, image_path: str, text: str, base_font_size: int = 58, position: str = 'top-center',
                          margin_top: Optional[int] = None, top_padding: Optional[int] = None) -> None:
        """
        Add text overlay to an image with dynamic font scaling based on image size.
        
        Parameters:
        -----------
        image_path : str
            Path to the image file to modify
        text : str
            Text to overlay on the image
        base_font_size : int, default=36
            Base font size for 1024x800 images (will be scaled proportionally for other sizes)
        position : str, default='top-center'
            Position for the text ('top-center', 'top-left', 'top-right', 'bottom-center', etc.)
        margin_top : Optional[int], default=None
            If set, use this as the top margin (px before scaling) for top-* positions. Smaller values place text higher (more clearance above the brain).
        top_padding : Optional[int], default=None
            If set, add a blank band of this height (px before scaling) above the image and draw text in it. Use when the brain fills the frame and text needs guaranteed clearance.
        """
        try:
            from PIL import Image, ImageDraw, ImageFont
            
            if not os.path.exists(image_path):
                print(f"Warning: Image not found for text overlay: {image_path}")
                return
            
            # Open the image
            img = Image.open(image_path).convert('RGB')
            img_width, img_height = img.size
            pad = 0
            
            # Optional: add top padding so text sits in a band above the brain
            if top_padding is not None and top_padding > 0:
                scale_factor_pad = max(img_width / 1024, img_height / 800)
                pad = int(top_padding * scale_factor_pad)
                new_img = Image.new('RGB', (img_width, img_height + pad), (255, 255, 255))
                new_img.paste(img, (0, pad))
                img.close()
                img = new_img
                draw = ImageDraw.Draw(img)
                # Total height now includes padding; text will go in the padding band
                img_height_total = img_height + pad
            else:
                draw = ImageDraw.Draw(img)
                img_height_total = img_height
            
            # Use 1024x800 as base resolution for scaling (based on original image)
            base_width = 1024
            base_height = 800
            width_ratio = img_width / base_width
            height_ratio = img_height / base_height
            scale_factor = max(width_ratio, height_ratio)
            
            scaled_font_size = int(base_font_size * scale_factor)
            
            print(f"Dynamic font scaling: {img_width}x{img_height} -> font size {scaled_font_size} (scale: {scale_factor:.2f})")
            
            # Try to use a nice font, fallback to default with size
            try:
                # Try common system fonts
                font_paths = [
                    '/System/Library/Fonts/Arial.ttf',  # macOS
                    '/System/Library/Fonts/Helvetica.ttc',  # macOS
                    '/System/Library/Fonts/Arial Bold.ttf',  # macOS
                    '/System/Library/Fonts/SF-Pro-Display-Bold.otf',  # macOS (modern)
                    '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',  # Linux
                    '/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf',  # Linux
                    'C:\\Windows\\Fonts\\arial.ttf',  # Windows
                    'C:\\Windows\\Fonts\\arialbd.ttf',  # Windows Bold
                ]
                
                font = None
                for font_path in font_paths:
                    if os.path.exists(font_path):
                        try:
                            font = ImageFont.truetype(font_path, scaled_font_size)
                            print(f"Using font: {font_path} at size {scaled_font_size}")
                            break
                        except Exception as e:
                            print(f"Failed to load font {font_path}: {e}")
                            continue
                
                # If no TrueType font found, try default font with size (Pillow 8.0+)
                if font is None:
                    try:
                        font = ImageFont.load_default(size=scaled_font_size)
                        print(f"Using default font with size {scaled_font_size}")
                    except TypeError:
                        # Older Pillow versions don't support size parameter
                        font = ImageFont.load_default()
                        print(f"Using default font (size parameter not supported in this Pillow version)")
                        
            except Exception as e:
                print(f"Error loading fonts: {e}")
                try:
                    font = ImageFont.load_default(size=scaled_font_size)
                    print(f"Using default font with size {scaled_font_size}")
                except TypeError:
                    font = ImageFont.load_default()
                    print(f"Using default font (size parameter not supported)")
            
            # Get text dimensions
            bbox = draw.textbbox((0, 0), text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
            
            # Calculate position with scaled margins
            margin = int(20 * scale_factor)  # Scale the 20px margin proportionally
            top_margin = int((margin_top if margin_top is not None else 20) * scale_factor)
            
            if pad > 0:
                # Text in the padding band: sit just above the brain with a small gap
                gap_above_brain = int(8 * scale_factor)
                y_top = max(2, pad - text_height - gap_above_brain)
                if position == 'top-center':
                    x = (img_width - text_width) // 2
                    y = y_top
                elif position == 'top-left':
                    x = margin
                    y = y_top
                elif position == 'top-right':
                    x = img_width - text_width - margin
                    y = y_top
                elif position == 'bottom-center':
                    x = (img_width - text_width) // 2
                    y = img_height_total - text_height - margin
                else:
                    x = (img_width - text_width) // 2
                    y = y_top
            else:
                if position == 'top-center':
                    x = (img_width - text_width) // 2
                    y = top_margin
                elif position == 'top-left':
                    x = margin
                    y = top_margin
                elif position == 'top-right':
                    x = img_width - text_width - margin
                    y = top_margin
                elif position == 'bottom-center':
                    x = (img_width - text_width) // 2
                    y = img_height_total - text_height - margin
                else:  # Default to top-center
                    x = (img_width - text_width) // 2
                    y = top_margin
            
            # Draw text with outline for better visibility
            # Draw outline (white) with scaled outline width
            outline_width = max(1, int(2 * scale_factor))  # Scale outline, minimum 1px
            for adj in range(-outline_width, outline_width + 1):
                for adj2 in range(-outline_width, outline_width + 1):
                    draw.text((x + adj, y + adj2), text, font=font, fill='white')
            
            # Draw main text (black)
            draw.text((x, y), text, font=font, fill='black')
            
            # Save the modified image
            img.save(image_path)
            img.close()
            
            print(f"Added text overlay '{text}' (font: {scaled_font_size}px, outline: {outline_width}px) to {os.path.basename(image_path)}")
            
        except Exception as e:
            print(f"Warning: Could not add text overlay to {image_path}: {e}")

    # ------------------------------------------------------------
    # Main visualization workflows: iterative, all_tracts, and gradient plot
    # ------------------------------------------------------------
    
    def _visualize_tracts_iterative(self, matched_tracts: List[str], color_scheme: str,
                                   values: Optional[List[float]], converted_color_dict: Optional[Dict[str, tuple]], 
                                   output_name: str, view: str, grid_orientation: Optional[str], colorbar: bool,
                                   has_single_color: bool, has_values_column: bool, values_column: Optional[str],
                                   has_color_column: bool, color_column: Optional[str], full_dataset_values: Optional[List[float]],
                                   full_dataset_colors: Optional[Dict[str, tuple]], keep_csv: bool, keep_color_files: bool, cleanup_renamed_files: bool,
                                   value_range: Optional[Tuple[float, float]] = None,
                                   colorbar_title: Optional[str] = None) -> None:
        """
        Process each tract individually using numbered files for consistent ordering.
        
        If grid_orientation is specified with view parameter:
            - Groups tracts by base name (e.g., AF_L and AF_R become one group)
            - Creates one grid per group showing both hemispheres with the specified view
        If grid_orientation is specified without view parameter:
            - Creates a grid (lateral + medial) for each individual tract
        If grid_orientation is None:
            - Creates single view per tract using 'view' parameter
        """
        
        # Create numbered tract files even for iterative mode for consistency
        numbered_tract_files, numbered_dir = self._create_numbered_tract_files(
            matched_tracts, values, converted_color_dict)
        
        try:
            # Create a mapping from tract names to their numbered files
            tract_to_numbered_file = {}
            for numbered_file in numbered_tract_files:
                basename = os.path.basename(numbered_file)
                # Extract original tract name from numbered filename (remove the prefix)
                for tract in matched_tracts:
                    if self._tract_basename_matches(basename, tract):
                        tract_to_numbered_file[tract] = numbered_file
                        break
            
            # Check if we need to group tracts by base name (for showing both hemispheres together)
            # This happens when grid_orientation is specified with a single view type
            if grid_orientation is not None and view in ['lateral', 'medial']:
                # Group tracts by base name (AF_L and AF_R -> AF; ...FasciculusL and ...FasciculusR -> ...Fasciculus)
                def _tract_base_for_group(tract: str) -> str:
                    if tract.endswith('_L') or tract.endswith('_R'):
                        return tract[:-2].rstrip('_.')
                    if tract and tract[-1] in ('L', 'R'):
                        return tract[:-1].rstrip('_.')
                    return tract
                tract_groups = {}
                for tract in matched_tracts:
                    base_name = _tract_base_for_group(tract)
                    if base_name not in tract_groups:
                        tract_groups[base_name] = []
                    tract_groups[base_name].append(tract)
                
                # Process each group (create one image per group showing both hemispheres)
                for group_idx, (base_name, group_tracts) in enumerate(tract_groups.items()):
                    print(f"Processing tract group {group_idx+1}/{len(tract_groups)}: {base_name} ({len(group_tracts)} hemisphere(s))")
                    
                    # If group has both L and R, create a horizontal grid
                    if len(group_tracts) == 2:
                        self._create_hemisphere_pair_grid(
                            group_tracts, base_name, tract_to_numbered_file, 
                            color_scheme, values, converted_color_dict, output_name,
                            view, grid_orientation, colorbar, has_single_color,
                            has_values_column, values_column, has_color_column, color_column,
                            full_dataset_values, full_dataset_colors, keep_csv, keep_color_files,
                            matched_tracts, value_range, colorbar_title
                        )
                    else:
                        # Single hemisphere - process normally (use index in matched_tracts for correct value lookup)
                        for tract in group_tracts:
                            tract_idx = matched_tracts.index(tract) if tract in matched_tracts else 0
                            self._process_single_tract_iterative(
                                tract_idx, tract, matched_tracts, tract_to_numbered_file,
                                color_scheme, values, converted_color_dict, output_name,
                                view, grid_orientation, colorbar, has_single_color,
                                has_values_column, values_column, has_color_column, color_column,
                                full_dataset_values, full_dataset_colors, keep_csv, keep_color_files,
                                value_range, colorbar_title
                            )
                return  # Exit early - we've processed all tracts in groups
        
            # Original iterative processing (one tract at a time)
            for i, tract in enumerate(matched_tracts):
                print(f"Processing tract {i+1}/{len(matched_tracts)}: {tract}")
                
                # Find the numbered file for this tract
                if tract not in tract_to_numbered_file:
                    print(f"Warning: Could not find numbered file for tract '{tract}'")
                    continue
                    
                numbered_tract_file = tract_to_numbered_file[tract]
                
                # Create color file for this single tract
                color_file = f'{self.output_dir}/{output_name}_{tract}_colors.txt'
                
                # Get the color value for this specific tract
                if converted_color_dict:
                    # Use converted color dictionary (keys are already in HCPYA_1065 format)
                    self._create_color_file([tract], color_file, color_scheme, None, converted_color_dict)
                else:
                    # Use values-based coloring (global value_range so same metric -> same color)
                    if values is not None and i < len(values):
                        tract_values = [values[i]]
                    else:
                        tract_values = [i / max(1, len(matched_tracts) - 1)]  # Normalize index
                    
                    self._create_color_file([tract], color_file, color_scheme, tract_values, value_range=value_range)
                
                # Check if we need to create a grid (both lateral and medial views) or single view
                if grid_orientation is not None:
                    # Create grid for this tract - may need both lateral and medial views or just one view type
                    final_image = self._create_iterative_tract_grid(tract, numbered_tract_file, color_file, 
                                                    output_name, grid_orientation, keep_csv, keep_color_files, view)
                    
                    # Add colorbar if requested
                    if colorbar and final_image:
                        # Use full dataset values for colorbar range, not just this tract's value
                        colorbar_values = full_dataset_values if full_dataset_values else ([values[i]] if values else None)
                        self._add_colorbar_to_image(final_image, [tract], color_scheme, 
                                                  colorbar_values, converted_color_dict,
                                                  has_single_color, has_values_column, values_column,
                                                  has_color_column, color_column, full_dataset_colors, colorbar_title)
                else:
                    # Create single view for this tract
                    final_image = self._create_single_tract_view(tract, numbered_tract_file, color_file, 
                                                 output_name, view, keep_csv, keep_color_files)
                    
                    # Add colorbar if requested
                    if colorbar and final_image:
                        # Use full dataset values for colorbar range, not just this tract's value
                        colorbar_values = full_dataset_values if full_dataset_values else ([values[i]] if values else None)
                        self._add_colorbar_to_image(final_image, [tract], color_scheme,
                                                  colorbar_values, converted_color_dict,
                                                  has_single_color, has_values_column, values_column,
                                                  has_color_column, color_column, full_dataset_colors, colorbar_title)
                    
        finally:
            # Clean up numbered files if requested
            if cleanup_renamed_files:
                print("Cleaning up numbered tract files...")
                if os.path.exists(numbered_dir):
                    shutil.rmtree(numbered_dir)
                    print(f"Removed directory: {numbered_dir}")
            else:
                print(f"Kept numbered tract files in: {numbered_dir}")
    
    def _visualize_all_tracts(self, matched_tracts: List[str], color_scheme: str,
                             values: Optional[List[float]], converted_color_dict: Optional[Dict[str, tuple]], 
                             output_name: str, grid_orientation: str, colorbar: bool,
                             has_single_color: bool, has_values_column: bool, values_column: Optional[str],
                             has_color_column: bool, color_column: Optional[str], full_dataset_values: Optional[List[float]],
                             full_dataset_colors: Optional[Dict[str, tuple]], keep_csv: bool, keep_color_files: bool, cleanup_renamed_files: bool,
                             view: str = 'lateral',
                             value_range: Optional[Tuple[float, float]] = None,
                             colorbar_title: Optional[str] = None) -> None:
        """Process all tracts together, creating a grid with hemisphere views.
        Creates 2x2 grid for both hemispheres (if grid_orientation and both views needed), 
        or 1x2 grid for single view type (if view specified), or 1x2/2x1 grid for single hemisphere.
        Uses numbered tract files to ensure correct loading order."""
        
        # Create numbered tract files to ensure correct loading order
        print("Creating numbered tract files for proper ordering...")
        numbered_tract_files, numbered_dir = self._create_numbered_tract_files(
            matched_tracts, values, converted_color_dict)
        
        try:
            # Separate tracts by hemisphere based on original tract names
            left_tracts = []
            right_tracts = []
            
            for tract in matched_tracts:
                if '_L' in tract or tract.endswith('_left'):
                    left_tracts.append(tract)
                elif '_R' in tract or tract.endswith('_right'):
                    right_tracts.append(tract)
                else:
                    # If hemisphere unclear, try to determine from file location
                    _, _, subfolder = self._find_tract_file(tract)
                    if subfolder == 'left_hem':
                        left_tracts.append(tract)
                    elif subfolder == 'right_hem':
                        right_tracts.append(tract)
                    elif subfolder == 'bilateral':
                        # Bilateral tracts: plot on both left and right hemisphere views
                        left_tracts.append(tract)
                        right_tracts.append(tract)
                    else:
                        print(f"Warning: Could not determine hemisphere for tract '{tract}' and no file found")
        
            print(f"Separated tracts - Left: {len(left_tracts)}, Right: {len(right_tracts)}")
            
            # Determine if we need both lateral and medial views or just one view type
            # If grid_orientation is None, we create both views (original behavior)
            # If view parameter is specified along with grid_orientation, respect the view setting
            views_to_create = ['lateral', 'medial'] if grid_orientation is None else [view]
            
            if grid_orientation is not None and view in ['lateral', 'medial']:
                print(f"Creating single view type ({view}) with grid_orientation={grid_orientation}")
            
            # Create views for each hemisphere
            hemisphere_images = {}
            
            if left_tracts:
                left_numbered_files = []
                for f in numbered_tract_files:
                    basename = os.path.basename(f)
                    for tract in left_tracts:
                        if self._tract_basename_matches(basename, tract):
                            left_numbered_files.append(f)
                            break
                hemisphere_images.update(self._create_hemisphere_views_for_grid(
                    left_tracts, 'left', left_numbered_files, matched_tracts, 
                    color_scheme, values, converted_color_dict, output_name, keep_csv, keep_color_files, 
                    views_to_create, value_range))
            
            if right_tracts:
                right_numbered_files = []
                for f in numbered_tract_files:
                    basename = os.path.basename(f)
                    for tract in right_tracts:
                        if self._tract_basename_matches(basename, tract):
                            right_numbered_files.append(f)
                            break
                hemisphere_images.update(self._create_hemisphere_views_for_grid(
                    right_tracts, 'right', right_numbered_files, matched_tracts, 
                    color_scheme, values, converted_color_dict, output_name, keep_csv, keep_color_files,
                    views_to_create, value_range))
            
            # Create grid from individual hemisphere views
            if hemisphere_images:
                # Determine grid type and output filename
                has_left = left_tracts and len(left_tracts) > 0
                has_right = right_tracts and len(right_tracts) > 0
                has_left_lateral = 'left_lateral' in hemisphere_images
                has_left_medial = 'left_medial' in hemisphere_images
                has_right_lateral = 'right_lateral' in hemisphere_images
                has_right_medial = 'right_medial' in hemisphere_images
                
                if has_left and has_right:
                    # Both hemispheres
                    # Check if we have both lateral and medial views or just one view type
                    if has_left_lateral and has_left_medial and has_right_lateral and has_right_medial:
                        # Full 2x2 grid with all views
                        grid_output = f'{self.output_dir}/{output_name}_all_hemispheres_grid.jpg'
                        left_lateral = hemisphere_images.get('left_lateral', '')
                        left_medial = hemisphere_images.get('left_medial', '')
                        right_lateral = hemisphere_images.get('right_lateral', '')
                        right_medial = hemisphere_images.get('right_medial', '')
                        
                        self._create_2x2_grid(left_lateral, left_medial, right_lateral, right_medial, 
                                            grid_output, grid_orientation)
                    elif has_left_lateral and has_right_lateral and not has_left_medial and not has_right_medial:
                        # Only lateral views - create 1x2 horizontal grid (left lateral, right lateral)
                        grid_output = f'{self.output_dir}/{output_name}_both_hemispheres_{view}.jpg'
                        left_lateral = hemisphere_images.get('left_lateral', '')
                        right_lateral = hemisphere_images.get('right_lateral', '')
                        
                        self._create_single_view_grid([left_lateral, right_lateral], grid_output, orientation='horizontal')
                        print(f"Created 1x2 horizontal grid with both hemisphere {view} views")
                    elif has_left_medial and has_right_medial and not has_left_lateral and not has_right_lateral:
                        # Only medial views - create 1x2 horizontal grid (left medial, right medial)
                        grid_output = f'{self.output_dir}/{output_name}_both_hemispheres_{view}.jpg'
                        left_medial = hemisphere_images.get('left_medial', '')
                        right_medial = hemisphere_images.get('right_medial', '')
                        
                        self._create_single_view_grid([left_medial, right_medial], grid_output, orientation='horizontal')
                        print(f"Created 1x2 horizontal grid with both hemisphere {view} views")
                    
                    # Add colorbar if requested
                    if colorbar:
                        all_tract_names = list(dict.fromkeys(left_tracts + right_tracts))  # dedupe bilateral
                        # Use full dataset values for colorbar range, not just selected tract values
                        colorbar_values = full_dataset_values if full_dataset_values else values
                        self._add_colorbar_to_image(grid_output, all_tract_names, color_scheme, colorbar_values, 
                                                  converted_color_dict, has_single_color, has_values_column, values_column,
                                                  has_color_column, color_column, full_dataset_colors, colorbar_title)
                    
                elif has_left:
                    # Only left hemisphere - create 1x2 or 2x1 grid
                    grid_output = f'{self.output_dir}/{output_name}_left_hemisphere_grid.jpg'
                    left_lateral = hemisphere_images.get('left_lateral', '')
                    left_medial = hemisphere_images.get('left_medial', '')
                    
                    self._create_single_hemisphere_grid(left_lateral, left_medial, 'left', 
                                                      grid_output, grid_orientation)
                    
                    # Add colorbar if requested
                    if colorbar:
                        # Use full dataset values for colorbar range, not just selected tract values
                        colorbar_values = full_dataset_values if full_dataset_values else values
                        self._add_colorbar_to_image(grid_output, left_tracts, color_scheme, colorbar_values, 
                                                  converted_color_dict, has_single_color, has_values_column, values_column,
                                                  has_color_column, color_column, full_dataset_colors, colorbar_title)
                    
                elif has_right:
                    # Only right hemisphere - create 1x2 or 2x1 grid
                    grid_output = f'{self.output_dir}/{output_name}_right_hemisphere_grid.jpg'
                    right_lateral = hemisphere_images.get('right_lateral', '')
                    right_medial = hemisphere_images.get('right_medial', '')
                    
                    self._create_single_hemisphere_grid(right_lateral, right_medial, 'right',
                                                      grid_output, grid_orientation)
                    
                    # Add colorbar if requested
                    if colorbar:
                        # Use full dataset values for colorbar range, not just selected tract values
                        colorbar_values = full_dataset_values if full_dataset_values else values
                        self._add_colorbar_to_image(grid_output, right_tracts, color_scheme, colorbar_values, 
                                                  converted_color_dict, has_single_color, has_values_column, values_column,
                                                  has_color_column, color_column, full_dataset_colors, colorbar_title)
                
                # Optionally clean up individual hemisphere images
                if not keep_csv:  # Use keep_csv flag to also control keeping individual images
                    for img_path in hemisphere_images.values():
                        if os.path.exists(img_path):
                            os.remove(img_path)
                    print("Cleaned up individual hemisphere images")
            else:
                print("Error: No valid hemisphere images created")
                
        finally:
            # Clean up numbered files if requested
            if cleanup_renamed_files:
                print("Cleaning up numbered tract files...")
                if os.path.exists(numbered_dir):
                    shutil.rmtree(numbered_dir)
                    print(f"Removed directory: {numbered_dir}")
            else:
                print(f"Kept numbered tract files in: {numbered_dir}")

    def _visualize_tract_gradient_plot(self, tract_df: Optional[pd.DataFrame], matched_tracts: List[str], color_scheme: str,
                                     values: Optional[List[float]], converted_color_dict: Optional[Dict[str, tuple]], 
                                     output_name: str, tract_name_column: str, tract_list: Optional[Union[List[str], str]],
                                     has_values_column: bool, values_column: Optional[str], colorbar: bool, view: Union[str, List[str]],
                                     gradient_n_tracts: int, has_single_color: bool, has_color_column: bool, color_column: Optional[str],
                                     full_dataset_values: Optional[List[float]], full_dataset_colors: Optional[Dict[str, tuple]], 
                                     keep_csv: bool, keep_color_files: bool, cleanup_renamed_files: bool,
                                     value_range: Optional[Tuple[float, float]] = None,
                                     colorbar_title: Optional[str] = None) -> None:
        """
        Create a gradient plot with flexible grid layout and view options.
        
        Handles different scenarios for tract selection and sorting:
        1. No tract_list + tract_df + values_column: Sort by values, select evenly spaced tracts
        2. tract_list + tract_df + values_column: Sort specified tracts by values  
        3. tract_list only: Plot tracts in specified order
        
        View options:
        - Single string (e.g., 'lateral'): Applied to all tracts in the gradient
        - List of strings: Must match the number of tracts, specifies view for each tract
        
        Grid layout automatically adjusts to accommodate the specified number of tracts.
        Colors are always based on full tract_df when available, not just plotted subset.
        """
        
        print(f"Creating tract gradient plot (flexible grid layout, up to {gradient_n_tracts} tracts)")
        
        # Determine which tracts to plot based on the scenario
        if tract_df is not None and has_values_column and values_column is not None:
            if tract_list is None or tract_list == 'all':
                # Scenario 1: No tract_list specified, use all tracts from DataFrame sorted by values
                print("Gradient plot: No tract_list specified, selecting evenly spaced tracts from sorted DataFrame")
                
                # Sort all tracts by values column
                tract_df_sorted = tract_df.sort_values(by=values_column, ascending=True)
                all_tract_names = tract_df_sorted[tract_name_column].tolist()
                all_values = tract_df_sorted[values_column].tolist()
                
                # Select evenly spaced tracts based on gradient_n_tracts parameter
                selected_tracts = self._select_evenly_spaced_tracts(
                    all_tract_names, 
                    max_tracts=gradient_n_tracts,
                    values=all_values,
                    values_column_name=values_column
                )
                
            else:
                # Scenario 2: tract_list specified with DataFrame and values_column
                print("Gradient plot: tract_list specified with DataFrame, sorting by values_column")
                
                # Filter DataFrame to only include specified tracts
                if isinstance(tract_list, str):
                    tract_list = [tract_list]
                
                tract_subset = tract_df[tract_df[tract_name_column].isin(tract_list)]
                
                if tract_subset.empty:
                    print("Warning: No matching tracts found in DataFrame for specified tract_list")
                    selected_tracts = tract_list  # Use all specified tracts
                else:
                    # Sort subset by values column and use all specified tracts (not limited by gradient_n_tracts)
                    tract_subset_sorted = tract_subset.sort_values(by=values_column, ascending=True)
                    selected_tracts = tract_subset_sorted[tract_name_column].tolist()
                    print(f"Sorted {len(selected_tracts)} tracts from tract_list by {values_column}")
        else:
            # Scenario 3: tract_list only (no DataFrame or no values_column)
            print("Gradient plot: Using tract_list in specified order")
            
            if tract_list is None:
                raise ValueError("tract_gradient_plot requires either tract_df with values_column, or tract_list")
            
            if isinstance(tract_list, str):
                if tract_list == 'all':
                    # Use gradient_n_tracts from all available tracts from abbreviations
                    selected_tracts = self._get_all_tracts_from_abbreviations()[:gradient_n_tracts]
                    print(f"Using first {gradient_n_tracts} tracts from available tract list")
                else:
                    selected_tracts = [tract_list]
            else:
                selected_tracts = tract_list  # Use all tracts in the specified list (no limit when tract_list is provided)
            
            print(f"Using {len(selected_tracts)} tracts in specified order")
        
        # Validate view parameter
        if isinstance(view, str):
            # Single view applied to all tracts
            tract_views = [view] * len(selected_tracts)
            print(f"Using '{view}' view for all {len(selected_tracts)} tracts")
        elif isinstance(view, list):
            # List of views for each tract
            if len(view) != len(selected_tracts):
                raise ValueError(f"view list length ({len(view)}) must match number of tracts to plot ({len(selected_tracts)})")
            tract_views = view
            print(f"Using individual views for {len(selected_tracts)} tracts: {tract_views}")
        else:
            raise ValueError(f"view must be a string or list of strings, got {type(view)}")
        
        # Match tract names to actual file names and create corresponding view list
        gradient_matched_tracts = []
        gradient_matched_views = []
        for i, tract in enumerate(selected_tracts):
            matched = self._match_tract_names([tract])
            if matched:
                gradient_matched_tracts.extend(matched)
                # Repeat the view for each matched tract (in case one original tract maps to multiple matched tracts)
                gradient_matched_views.extend([tract_views[i]] * len(matched))
        
        if not gradient_matched_tracts:
            print("Error: No valid tracts found for gradient plot")
            return
            
        print(f"Final gradient plot will include {len(gradient_matched_tracts)} matched tracts")
        print(f"Views for matched tracts: {gradient_matched_views}")
        
        # Create color dictionary for gradient tracts based on full dataset
        if converted_color_dict:
            # Use existing color dictionary (colors already computed from full dataset)
            gradient_color_dict = {tract: converted_color_dict[tract] for tract in gradient_matched_tracts 
                                 if tract in converted_color_dict}
            print(f"Using pre-computed colors for {len(gradient_color_dict)} gradient tracts")
        else:
            # Create colors based on full dataset context
            if tract_df is not None and has_values_column and values_column is not None:
                # Map gradient tract values from full DataFrame context
                gradient_values = []
                full_df_sorted = tract_df.sort_values(by=values_column, ascending=True)
                
                for tract_name in selected_tracts:
                    tract_row = full_df_sorted[full_df_sorted[tract_name_column] == tract_name]
                    if not tract_row.empty:
                        gradient_values.append(tract_row[values_column].iloc[0])
                    else:
                        # Fallback: use position-based value
                        idx = selected_tracts.index(tract_name)
                        gradient_values.append(idx / max(1, len(selected_tracts) - 1))
                
                print(f"Using values from full dataset context for gradient coloring")
            else:
                # Position-based values for gradient
                gradient_values = [i / max(1, len(selected_tracts) - 1) for i in range(len(selected_tracts))]
                print(f"Using position-based values for gradient coloring")
            
            gradient_color_dict = None  # Let the color file creation handle it
        
        # Create numbered tract files for consistent loading order
        numbered_tract_files, numbered_dir = self._create_numbered_tract_files(
            gradient_matched_tracts, gradient_values if 'gradient_values' in locals() else None, gradient_color_dict)
        
        try:
            # Generate individual tract images (lateral view only)
            gradient_images = []
            
            for i, tract in enumerate(gradient_matched_tracts):
                # Print tract info with value if available
                if 'gradient_values' in locals() and i < len(gradient_values) and values_column:
                    print(f"Processing gradient tract {i+1}/{len(gradient_matched_tracts)}: {tract} ({values_column} = {gradient_values[i]:.3f})")
                else:
                    print(f"Processing gradient tract {i+1}/{len(gradient_matched_tracts)}: {tract}")
                
                # Find numbered file for this tract
                numbered_file = None
                for nf in numbered_tract_files:
                    basename = os.path.basename(nf)
                    if self._tract_basename_matches(basename, tract):
                        numbered_file = nf
                        break
                
                if not numbered_file:
                    print(f"Warning: Could not find numbered file for tract '{tract}'")
                    continue
                
                # Create color file for this tract
                color_file = f'{self.output_dir}/{output_name}_gradient_{tract}_colors.txt'
                
                if gradient_color_dict:
                    self._create_color_file([tract], color_file, color_scheme, None, gradient_color_dict)
                else:
                    if 'gradient_values' in locals() and i < len(gradient_values):
                        tract_values = [gradient_values[i]]
                    else:
                        tract_values = [i / max(1, len(gradient_matched_tracts) - 1)]
                    
                    self._create_color_file([tract], color_file, color_scheme, tract_values, value_range=value_range)
                
                # Get the appropriate view for this tract
                tract_view = gradient_matched_views[i] if i < len(gradient_matched_views) else 'lateral'
                
                # Create output image with appropriate view
                tract_output_name = f'{output_name}_gradient_{tract}_{tract_view}'
                output_image = f'{self.output_dir}/{tract_output_name}.jpg'
                
                # Remove existing image to ensure fresh generation with updated text overlay
                if os.path.exists(output_image):
                    os.remove(output_image)
                    print(f"Removed existing image to ensure fresh generation: {os.path.basename(output_image)}")
                
                # Create and run DSI Studio command
                csv_command_file = f"{self.output_dir}/temp_gradient_command_{tract.replace('/', '_')}.csv"
                self._create_dsi_command_csv(csv_command_file, numbered_file, color_file, output_image, view_type=tract_view)
                self._run_dsi_command(f"gradient_{tract}", csv_command_file, output_image, color_file, keep_csv, keep_color_files)
                
                # Add tract abbreviation overlay (top_padding for ICBM152 so text sits in a band above the brain)
                tract_abbreviation = self._get_tract_abbreviation(tract)
                if 'ICBM152' in self.fib_file:
                    self._add_text_overlay(output_image, tract_abbreviation, top_padding=55)
                else:
                    self._add_text_overlay(output_image, tract_abbreviation)
                
                gradient_images.append(output_image)
                
                # Clean up color file unless keep_color_files is True
                if not keep_color_files and os.path.exists(color_file):
                    os.remove(color_file)
            
            # Create optimal grid from all gradient images
            if gradient_images:
                final_output = f'{self.output_dir}/{output_name}_tract_gradient.jpg'
                self._create_gradient_grid(gradient_images, final_output)
                
                # Add colorbar if requested
                if colorbar:
                    # Use pre-computed full dataset values for proper colorbar range
                    colorbar_values = full_dataset_values if full_dataset_values else (gradient_values if 'gradient_values' in locals() else None)
                    
                    self._add_colorbar_to_image(final_output, gradient_matched_tracts, color_scheme, 
                                              colorbar_values, gradient_color_dict if 'gradient_color_dict' in locals() else converted_color_dict,
                                              has_single_color, has_values_column, values_column,
                                              has_color_column, color_column, full_dataset_colors, colorbar_title)
                
                # Clean up individual images (keep only the final grid)
                for img_path in gradient_images:
                    if os.path.exists(img_path):
                        os.remove(img_path)
                
                print(f"Created tract gradient plot with {len(gradient_images)} tracts: {final_output}")
            else:
                print("Error: No gradient images were created")
                
        finally:
            # Clean up numbered files if requested
            if cleanup_renamed_files:
                print("Cleaning up numbered tract files...")
                if os.path.exists(numbered_dir):
                    shutil.rmtree(numbered_dir)
                    print(f"Removed directory: {numbered_dir}")
            else:
                print(f"Kept numbered tract files in: {numbered_dir}")

    # ------------------------------------------------------------
    # Helper methods for iterative processing
    # ------------------------------------------------------------

    def _create_hemisphere_pair_grid(self, group_tracts: List[str], base_name: str, 
                                    tract_to_numbered_file: Dict[str, str],
                                    color_scheme: str, values: Optional[List[float]], 
                                    converted_color_dict: Optional[Dict[str, tuple]],
                                    output_name: str, view: str, grid_orientation: str, 
                                    colorbar: bool, has_single_color: bool, 
                                    has_values_column: bool, values_column: Optional[str],
                                    has_color_column: bool, color_column: Optional[str],
                                    full_dataset_values: Optional[List[float]], 
                                    full_dataset_colors: Optional[Dict[str, tuple]],
                                    keep_csv: bool, keep_color_files: bool,
                                    all_matched_tracts: List[str],
                                    value_range: Optional[Tuple[float, float]] = None,
                                    colorbar_title: Optional[str] = None) -> None:
        """Create a grid showing both hemispheres of a tract with a single view type (lateral or medial)."""
        
        # Sort to ensure left comes before right
        group_tracts_sorted = sorted(group_tracts, key=lambda x: (not x.endswith('_L'), x))
        
        view_images = []
        
        for tract in group_tracts_sorted:
            if tract not in tract_to_numbered_file:
                print(f"Warning: Could not find numbered file for tract '{tract}'")
                continue
            
            numbered_tract_file = tract_to_numbered_file[tract]
            
            # Create color file for this tract
            color_file = f'{self.output_dir}/{output_name}_{tract}_colors.txt'
            
            # Get tract index in the original matched_tracts list for value lookup
            tract_idx = all_matched_tracts.index(tract) if tract in all_matched_tracts else 0
            
            # Get the color value for this specific tract
            if converted_color_dict:
                self._create_color_file([tract], color_file, color_scheme, None, converted_color_dict)
            else:
                if values is not None and tract_idx < len(values):
                    tract_values = [values[tract_idx]]
                else:
                    tract_values = [tract_idx / max(1, len(all_matched_tracts) - 1)]
                self._create_color_file([tract], color_file, color_scheme, tract_values, value_range=value_range)
            
            # Create the specified view image
            view_image = f'{self.output_dir}/{output_name}_{tract}_{view}.jpg'
            csv_command_file = f"{self.output_dir}/temp_command_{tract.replace('/', '_')}_{view}.csv"
            
            self._create_dsi_command_csv(csv_command_file, numbered_tract_file, color_file, view_image, view_type=view)
            self._run_dsi_command(f"{tract}_{view}", csv_command_file, view_image, color_file, keep_csv, keep_color_files)
            
            # Clean up color file
            if not keep_color_files and os.path.exists(color_file):
                os.remove(color_file)
            
            view_images.append(view_image)
        
        # Create grid from the view images
        if len(view_images) == 2:
            grid_output = f'{self.output_dir}/{output_name}_{base_name}_{view}.jpg'
            self._create_single_view_grid(
                view_images,
                grid_output,
                orientation=grid_orientation,
            )

            # Add colorbar if requested
            if colorbar:
                colorbar_values = (
                    full_dataset_values if full_dataset_values else values
                )
                self._add_colorbar_to_image(
                    grid_output,
                    group_tracts_sorted,
                    color_scheme,
                    colorbar_values,
                    converted_color_dict,
                    has_single_color,
                    has_values_column,
                    values_column,
                    has_color_column,
                    color_column,
                    full_dataset_colors,
                    colorbar_title,
                )

            # Clean up individual view images
            for img_path in view_images:
                if os.path.exists(img_path):
                    os.remove(img_path)

            print(
                f"Created {view} {grid_orientation} grid for "
                f"{base_name}: {grid_output}"
            )

    def _process_single_tract_iterative(
        self,
        i: int,
        tract: str,
        all_matched_tracts: List[str],
        tract_to_numbered_file: Dict[str, str],
        color_scheme: str,
        values: Optional[List[float]],
        converted_color_dict: Optional[Dict[str, tuple]],
        output_name: str,
        view: str,
        grid_orientation: Optional[str],
        colorbar: bool,
        has_single_color: bool,
        has_values_column: bool,
        values_column: Optional[str],
        has_color_column: bool,
        color_column: Optional[str],
        full_dataset_values: Optional[List[float]],
        full_dataset_colors: Optional[Dict[str, tuple]],
        keep_csv: bool,
        keep_color_files: bool,
        value_range: Optional[Tuple[float, float]] = None,
        colorbar_title: Optional[str] = None,
    ) -> None:
        """Process one tract in iterative mode from the main plotting loop."""

        print(f"Processing tract {i+1}/{len(all_matched_tracts)}: {tract}")

        # Find the numbered file for this tract
        if tract not in tract_to_numbered_file:
            print(f"Warning: Could not find numbered file for tract '{tract}'")
            return

        numbered_tract_file = tract_to_numbered_file[tract]

        # Create color file for this single tract
        color_file = f"{self.output_dir}/{output_name}_{tract}_colors.txt"

        # Get the color value for this specific tract
        if converted_color_dict:
            self._create_color_file(
                [tract], color_file, color_scheme, None, converted_color_dict
            )
        else:
            if values is not None and i < len(values):
                tract_values = [values[i]]
            else:
                tract_values = [i / max(1, len(all_matched_tracts) - 1)]
            self._create_color_file(
                [tract],
                color_file,
                color_scheme,
                tract_values,
                value_range=value_range,
            )

        # Check if we need to create a grid or single view
        if grid_orientation is not None:
            final_image = self._create_iterative_tract_grid(
                tract,
                numbered_tract_file,
                color_file,
                output_name,
                grid_orientation,
                keep_csv,
                keep_color_files,
                view,
            )

            if colorbar and final_image:
                colorbar_values = (
                    full_dataset_values
                    if full_dataset_values
                    else ([values[i]] if values else None)
                )
                self._add_colorbar_to_image(
                    final_image,
                    [tract],
                    color_scheme,
                    colorbar_values,
                    converted_color_dict,
                    has_single_color,
                    has_values_column,
                    values_column,
                    has_color_column,
                    color_column,
                    full_dataset_colors,
                    colorbar_title,
                )
        else:
            final_image = self._create_single_tract_view(
                tract,
                numbered_tract_file,
                color_file,
                output_name,
                view,
                keep_csv,
                keep_color_files,
            )

            if colorbar and final_image:
                colorbar_values = (
                    full_dataset_values
                    if full_dataset_values
                    else ([values[i]] if values else None)
                )
                self._add_colorbar_to_image(
                    final_image,
                    [tract],
                    color_scheme,
                    colorbar_values,
                    converted_color_dict,
                    has_single_color,
                    has_values_column,
                    values_column,
                    has_color_column,
                    color_column,
                    full_dataset_colors,
                    colorbar_title,
                )

    # ------------------------------------------------------------
    # DSI Studio individual tract view
    # ------------------------------------------------------------

    def _create_single_tract_view(self, tract: str, tract_file: str, color_file: str, 
                                output_name: str, view: str, keep_csv: bool, keep_color_files: bool) -> str:
        """Create a single view (lateral or medial) for one tract in iterative mode.
        
        Returns:
            str: Path to the created image file
        """
        
        # Create output image name for this tract
        tract_output_name = f'{output_name}_{tract}'
        output_image = f'{self.output_dir}/{tract_output_name}.jpg'
        
        # Create temporary CSV command file for DSI Studio
        csv_command_file = f"{self.output_dir}/temp_command_{tract.replace('/', '_')}.csv"
        
        # Create DSI Studio command csv file with specific view
        # The _create_dsi_command_csv method expects 'lateral' or 'medial' strings
        # and automatically detects hemisphere from tract file names
        self._create_dsi_command_csv(
            csv_command_file,
            tract_file,
            color_file,
            output_image,
            view_type=view
        )
        
        # Run DSI Studio command
        self._run_dsi_command(tract, csv_command_file, output_image, color_file, keep_csv, keep_color_files)
        
        # Clean up color file unless keep_color_files is True
        if not keep_color_files and os.path.exists(color_file):
            os.remove(color_file)
            print(f"Cleaned up color file: {color_file}")
        elif keep_color_files and os.path.exists(color_file):
            print(f"Kept color file for debugging: {color_file}")
        
        return output_image
    
    def _create_iterative_tract_grid(self, tract: str, tract_file: str, color_file: str,
                                   output_name: str, grid_orientation: str, keep_csv: bool, keep_color_files: bool,
                                   view: str = 'lateral') -> str:
        """Create a grid for one tract in iterative mode. 
        
        Can create either:
        - Grid with both lateral and medial views (if view not specified or both needed)
        - Grid with only one view type for left and right hemispheres (if view='lateral' or 'medial')
        
        Orientation is specified by the grid_orientation parameter (e.g., "vertical", "horizontal").
        
        Parameters:
        -----------
        view : str
            View type: 'lateral' or 'medial'. Determines whether to create both views or single view type.
        
        Returns:
        --------
        str: Path to the created grid image file
        """
        
        tract_output_base = f'{output_name}_{tract}'
        
        # Determine hemisphere for grid creation
        is_left = tract.endswith('_L')
        is_right = tract.endswith('_R')
        
        if is_left:
            hemisphere = 'left'
        elif is_right:
            hemisphere = 'right'
        else:
            # Default to left hemisphere for non-standard tract names
            hemisphere = 'left'
        
        # Check if this tract abbreviation has both L and R versions that need to be shown together
        # This happens when the user passes an abbreviation like 'AF' which expands to both AF_L and AF_R
        tract_base = tract.replace('_L', '').replace('_R', '')
        
        # For iterative mode with grid_orientation and single view, we want to show BOTH hemispheres
        # So we need to find the paired tract (L/R counterpart) if it exists
        # This allows showing AF_L and AF_R lateral views side by side in iterative mode
        
        # For now, just create the standard single hemisphere grid (lateral + medial)
        # The user needs to use all_tracts mode to get both hemispheres in one image
        
        lateral_image = f'{self.output_dir}/{tract_output_base}_lateral.jpg'
        medial_image = f'{self.output_dir}/{tract_output_base}_medial.jpg'
        
        # Create lateral view (always 'lateral' string, hemisphere detection is automatic)
        csv_lateral = f"{self.output_dir}/temp_command_{tract.replace('/', '_')}_lateral.csv"
        self._create_dsi_command_csv(csv_lateral, tract_file, color_file, lateral_image, view_type='lateral')
        self._run_dsi_command(f"{tract}_lateral", csv_lateral, lateral_image, color_file, keep_csv, keep_color_files)
        
        # Create medial view (always 'medial' string, hemisphere detection is automatic)
        csv_medial = f"{self.output_dir}/temp_command_{tract.replace('/', '_')}_medial.csv"
        self._create_dsi_command_csv(csv_medial, tract_file, color_file, medial_image, view_type='medial')
        self._run_dsi_command(f"{tract}_medial", csv_medial, medial_image, color_file, keep_csv, keep_color_files)
        
        # Clean up color file after both views are complete (unless keep_color_files is True)
        if not keep_color_files and os.path.exists(color_file):
            os.remove(color_file)
            print(f"Cleaned up color file after grid creation: {color_file}")
        elif keep_color_files and os.path.exists(color_file):
            print(f"Kept color file for debugging: {color_file}")
        
        # Create grid from the two views
        final_output = f'{self.output_dir}/{tract_output_base}.jpg'
        self._create_single_hemisphere_grid(lateral_image, medial_image, hemisphere, 
                                          final_output, grid_orientation)
        
        # Clean up individual view images (keep only the final grid)
        for img_path in [lateral_image, medial_image]:
            if os.path.exists(img_path):
                os.remove(img_path)
        
        print(f"Created {grid_orientation} grid for {tract}: {final_output}")
        
        return final_output

    # ------------------------------------------------------------
    # Hemisphere grid creation
    # ------------------------------------------------------------

    def _create_hemisphere_views_for_grid(self, hemisphere_tracts: List[str], hemisphere: str, 
                                        numbered_tract_files: List[str], all_matched_tracts: List[str], 
                                        color_scheme: str, values: Optional[List[float]], 
                                        converted_color_dict: Optional[Dict[str, tuple]],
                                        output_name: str, keep_csv: bool, keep_color_files: bool,
                                        views_to_create: List[str] = None,
                                        value_range: Optional[Tuple[float, float]] = None) -> Dict[str, str]:
        """Create specified views (lateral and/or medial) for a hemisphere and return image paths.
        
        Parameters:
        -----------
        views_to_create : List[str], optional
            List of view types to create: ['lateral'], ['medial'], or ['lateral', 'medial']
            If None, creates both lateral and medial views (default behavior)
        """
        if views_to_create is None:
            views_to_create = ['lateral', 'medial']
        
        print(f"Creating {', '.join(views_to_create)} view(s) for {hemisphere} hemisphere with numbered files...")
        
        # Create color file for this hemisphere (colors should be in the same order as numbered files)
        color_file = f'{self.output_dir}/{output_name}_{hemisphere}_hemisphere_colors.txt'
        
        # Filter the tract list and values/colors to match this hemisphere only
        hemisphere_tract_list = []
        hemisphere_values = []
        hemisphere_color_dict = {}
        
        for i, tract in enumerate(all_matched_tracts):
            if tract in hemisphere_tracts:
                hemisphere_tract_list.append(tract)
                if values is not None and i < len(values):
                    hemisphere_values.append(values[i])
                if converted_color_dict and tract in converted_color_dict:
                    hemisphere_color_dict[tract] = converted_color_dict[tract]
        
        # Create color file with hemisphere-specific data (use global value_range so same value -> same color)
        self._create_color_file(
            hemisphere_tract_list, 
            color_file, 
            color_scheme, 
            hemisphere_values if hemisphere_values else None,
            hemisphere_color_dict if hemisphere_color_dict else None,
            value_range=value_range
        )
        
        # Create requested views
        image_paths = {}
        
        # Create lateral view if requested
        if 'lateral' in views_to_create:
            lateral_output = f'{self.output_dir}/{output_name}_{hemisphere}_hemisphere_lateral.jpg'
            self._create_hemisphere_view(hemisphere_tracts, hemisphere, numbered_tract_files, color_file, 
                                       output_name, 'lateral', keep_csv, lateral_output)
            image_paths[f'{hemisphere}_lateral'] = lateral_output
        
        # Create medial view if requested
        if 'medial' in views_to_create:
            medial_output = f'{self.output_dir}/{output_name}_{hemisphere}_hemisphere_medial.jpg'
            self._create_hemisphere_view(hemisphere_tracts, hemisphere, numbered_tract_files, color_file,
                                       output_name, 'medial', keep_csv, medial_output)
            image_paths[f'{hemisphere}_medial'] = medial_output
        
        # Clean up color file unless keep_color_files is True
        try:
            if not keep_color_files and os.path.exists(color_file):
                os.remove(color_file)
            elif keep_color_files and os.path.exists(color_file):
                print(f"Kept color file for debugging: {color_file}")
        except Exception as e:
            print(f"Error during color file cleanup: {e}")
            
        return image_paths
    
    def _create_hemisphere_view(self, hemisphere_tracts: List[str], hemisphere: str, tract_files: List[str],
                              color_file: str, output_name: str, view_type: str, keep_csv: bool, 
                              output_image: Optional[str] = None) -> None:
        """Create a single view (lateral or medial) for hemisphere tracts.
        
        Parameters:
        -----------
        output_image : Optional[str]
            Custom output image path. If None, constructs path from output_name, hemisphere, and view_type.
        """
        
        # Construct output image path if not provided
        if output_image is None:
            output_image = f'{self.output_dir}/{output_name}_{hemisphere}_hemisphere_{view_type}.jpg'
        
        csv_command_file = f"{self.output_dir}/temp_command_{hemisphere}_hemisphere_{view_type}.csv"
        
        # Use consolidated CSV creation method
        self._create_dsi_command_csv(csv_command_file, tract_files, color_file, output_image, view_type)
        
        # Run DSI Studio
        cmd = f"{self.dsi_studio_path} {csv_command_file}"
        print(f"Running DSI Studio for {hemisphere} hemisphere {view_type} view: {csv_command_file}")
        
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                print(f"Successfully created {view_type} view for {hemisphere} hemisphere: {output_image}")
                
                # Ensure output image is exactly 1024x800 (DSI Studio sometimes outputs larger)
                self._ensure_image_dimensions(output_image, target_width=1024, target_height=800)
                
            else:
                print(f"DSI Studio error for {hemisphere} hemisphere {view_type} view: {result.stderr}")
                print(f"DSI Studio stdout for {hemisphere} hemisphere {view_type} view: {result.stdout}")
        except subprocess.TimeoutExpired:
            print(f"DSI Studio command timed out for {hemisphere} hemisphere {view_type} view (15 second timeout)")
        except Exception as e:
            print(f"Error running DSI Studio for {hemisphere} hemisphere {view_type} view: {e}")
        finally:
            # Clean up CSV file unless keep_csv is True
            if not keep_csv and os.path.exists(csv_command_file):
                os.remove(csv_command_file)
            elif keep_csv and os.path.exists(csv_command_file):
                print(f"Kept CSV command file for debugging: {csv_command_file}")

    # ------------------------------------------------------------
    # Grid creation
    # ------------------------------------------------------------

    def _create_2x2_grid(self, left_lateral: str, left_medial: str, 
                        right_lateral: str, right_medial: str, output_path: str,
                        orientation: str = 'vertical') -> None:
        """
        Combine 4 hemisphere views into a 2x2 grid layout.
        
        Vertical orientation (default):
        +----------------+----------------+
        | Left Lateral   | Right Lateral  |
        +----------------+----------------+
        | Left Medial    | Right Medial   |
        +----------------+----------------+
        
        Horizontal orientation:
        +----------------+----------------+
        | Left Lateral   | Left Medial    |
        +----------------+----------------+
        | Right Lateral  | Right Medial   |
        +----------------+----------------+
        
        Parameters:
        -----------
        left_lateral : str
            Path to left hemisphere lateral view image
        left_medial : str
            Path to left hemisphere medial view image  
        right_lateral : str
            Path to right hemisphere lateral view image
        right_medial : str
            Path to right hemisphere medial view image
        output_path : str
            Path to save the combined grid image
        orientation : str
            Grid orientation: 'vertical' or 'horizontal' (default: 'vertical')
        """
        try:
            # Load images and check their actual sizes
            images = {}
            image_paths = {
                'left_lateral': left_lateral,
                'left_medial': left_medial,
                'right_lateral': right_lateral,
                'right_medial': right_medial
            }
            
            # Load all images and track their sizes
            sizes = []
            for key, path in image_paths.items():
                if os.path.exists(path):
                    images[key] = Image.open(path)
                    size = images[key].size
                    sizes.append(size)
                    print(f"Loaded {key}: {size}")
                else:
                    print(f"Warning: Image not found: {path}")
                    # Create a placeholder image with standard DSI Studio size
                    images[key] = Image.new('RGB', (1024, 800), color='white')
                    sizes.append((1024, 800))
            
            # Force all images to be exactly 1024x800 (resize if necessary)
            target_width, target_height = 1024, 800
            print(f"Grid will use standard dimensions: {target_width} x {target_height} per image")
            
            # Ensure all images are exactly 1024x800 (resize if necessary)
            for key, img in images.items():
                current_size = img.size
                if current_size != (target_width, target_height):
                    # Resize image to exact target dimensions
                    resized_img = img.resize((target_width, target_height), Image.Resampling.LANCZOS)
                    img.close()  # Close the original
                    images[key] = resized_img
                    print(f"Resized {key} from {current_size} to ({target_width}, {target_height})")
                else:
                    print(f"Image {key} already correct size: {current_size}")
            
            # Create grid image (2x2) using consistent dimensions
            grid_width = target_width * 2
            grid_height = target_height * 2
            grid_image = Image.new('RGB', (grid_width, grid_height), color='white')
            
            # Paste images into grid positions based on orientation
            if orientation == 'vertical':
                # Vertical layout: lateral on top row, medial on bottom row
                positions = {
                    'left_lateral': (0, 0),                      # Top left
                    'right_lateral': (target_width, 0),          # Top right
                    'left_medial': (0, target_height),           # Bottom left
                    'right_medial': (target_width, target_height) # Bottom right
                }
                print(f"Using vertical layout (lateral top, medial bottom)")
            else:  # horizontal
                # Horizontal layout: lateral on left column, medial on right column
                positions = {
                    'left_lateral': (0, 0),                      # Top left
                    'left_medial': (target_width, 0),            # Top right
                    'right_lateral': (0, target_height),         # Bottom left
                    'right_medial': (target_width, target_height) # Bottom right
                }
                print(f"Using horizontal layout (lateral left, medial right)")
            
            for key, pos in positions.items():
                if key in images:
                    grid_image.paste(images[key], pos)
                    print(f"Placed {key} at position {pos}")
            
            # Save grid image
            grid_image.save(output_path, quality=95)
            print(f"Created 2x2 grid image ({grid_width}x{grid_height}): {output_path}")
            
            # Clean up individual images
            for img in images.values():
                img.close()
                
        except Exception as e:
            print(f"Error creating 2x2 grid: {e}")
            import traceback
            traceback.print_exc()

    def _create_single_hemisphere_grid(self, lateral_image: str, medial_image: str, 
                                     hemisphere: str, output_path: str, orientation: str = 'vertical') -> None:
        """
        Combine 2 hemisphere views into a 1x2 or 2x1 grid layout.
        
        Vertical orientation (1x2):
        +----------------+
        | Lateral        |
        +----------------+
        | Medial         |
        +----------------+
        
        Horizontal orientation (2x1):
        +----------------+----------------+
        | Lateral        | Medial         |
        +----------------+----------------+
        
        Parameters:
        -----------
        lateral_image : str
            Path to hemisphere lateral view image
        medial_image : str
            Path to hemisphere medial view image
        hemisphere : str
            Hemisphere name ('left' or 'right')
        output_path : str
            Path to save the combined grid image
        orientation : str
            Grid orientation: 'vertical' or 'horizontal' (default: 'vertical')
        """
        try:
            # Load images
            images = {}
            image_paths = {
                'lateral': lateral_image,
                'medial': medial_image
            }
            
            # Load all images and track their sizes
            sizes = []
            for key, path in image_paths.items():
                if os.path.exists(path):
                    images[key] = Image.open(path)
                    size = images[key].size
                    sizes.append(size)
                    print(f"Loaded {hemisphere} {key}: {size}")
                else:
                    print(f"Warning: Image not found: {path}")
                    # Create a placeholder image with standard DSI Studio size
                    images[key] = Image.new('RGB', (1024, 800), color='white')
                    sizes.append((1024, 800))
            
            # Force all images to be exactly 1024x800 (resize if necessary)
            target_width, target_height = 1024, 800
            print(f"Single hemisphere grid will use standard dimensions: {target_width} x {target_height} per image")
            
            # Ensure all images are exactly 1024x800 (resize if necessary)
            for key, img in images.items():
                current_size = img.size
                if current_size != (target_width, target_height):
                    # Resize image to exact target dimensions
                    resized_img = img.resize((target_width, target_height), Image.Resampling.LANCZOS)
                    img.close()  # Close the original
                    images[key] = resized_img
                    print(f"Resized {hemisphere} {key} from {current_size} to ({target_width}, {target_height})")
                else:
                    print(f"Image {hemisphere} {key} already correct size: {current_size}")
            
            # Create grid image based on orientation
            if orientation == 'vertical':
                # Vertical layout: 1x2 (lateral top, medial bottom)
                grid_width = target_width
                grid_height = target_height * 2
                positions = {
                    'lateral': (0, 0),                # Top
                    'medial': (0, target_height)      # Bottom
                }
                print(f"Creating vertical single hemisphere grid: {grid_width}x{grid_height}")
            else:  # horizontal
                # Horizontal layout: 2x1 (lateral left, medial right)
                grid_width = target_width * 2
                grid_height = target_height
                positions = {
                    'lateral': (0, 0),                # Left
                    'medial': (target_width, 0)       # Right
                }
                print(f"Creating horizontal single hemisphere grid: {grid_width}x{grid_height}")
            
            grid_image = Image.new('RGB', (grid_width, grid_height), color='white')
            
            # Paste images into grid positions
            for key, pos in positions.items():
                if key in images:
                    grid_image.paste(images[key], pos)
                    print(f"Placed {hemisphere} {key} at position {pos}")
            
            # Save grid image
            grid_image.save(output_path, quality=95)
            print(f"Created {orientation} single hemisphere grid ({grid_width}x{grid_height}): {output_path}")
            
            # Clean up individual images
            for img in images.values():
                img.close()
                
        except Exception as e:
            print(f"Error creating single hemisphere grid: {e}")
            import traceback
            traceback.print_exc()
    

    def _create_single_view_grid(self, image_paths: List[str], output_path: str, orientation: str = 'horizontal') -> None:
        """Combine multiple images of the same view type into a grid.
        
        Parameters:
        -----------
        image_paths : List[str]
            List of image paths to combine (e.g., [left_lateral, right_lateral])
        output_path : str
            Path to save the combined grid
        orientation : str
            'horizontal' for side-by-side (1 row, N cols) or 'vertical' for stacked (N rows, 1 col)
        """
        try:
            if not image_paths:
                print("Error: No images provided for single-view grid")
                return
            
            # Load all images
            images = []
            for img_path in image_paths:
                if os.path.exists(img_path):
                    img = Image.open(img_path)
                    images.append(img)
                    print(f"Loaded image: {os.path.basename(img_path)} ({img.width}x{img.height})")
                else:
                    print(f"Warning: Image not found: {img_path}")
                    # Create placeholder
                    images.append(Image.new('RGB', (1024, 800), color='white'))
            
            if not images:
                print("Error: No valid images found for single-view grid")
                return
            
            # Force all images to be exactly 1024x800
            target_width, target_height = 1024, 800
            resized_images = []
            
            for i, img in enumerate(images):
                if img.size != (target_width, target_height):
                    resized_img = img.resize((target_width, target_height), Image.Resampling.LANCZOS)
                    img.close()
                    resized_images.append(resized_img)
                    print(f"Resized image {i+1} from {img.size} to ({target_width}, {target_height})")
                else:
                    resized_images.append(img)
            
            # Create grid based on orientation
            if orientation == 'horizontal':
                # Horizontal layout: all images side by side (1 row, N columns)
                grid_width = target_width * len(resized_images)
                grid_height = target_height
                grid_image = Image.new('RGB', (grid_width, grid_height), color='white')
                
                for i, img in enumerate(resized_images):
                    x = i * target_width
                    grid_image.paste(img, (x, 0))
                    img.close()
                
                print(f"Created horizontal grid: {grid_width}x{grid_height} (1 row x {len(resized_images)} columns)")
            else:  # vertical
                # Vertical layout: all images stacked (N rows, 1 column)
                grid_width = target_width
                grid_height = target_height * len(resized_images)
                grid_image = Image.new('RGB', (grid_width, grid_height), color='white')
                
                for i, img in enumerate(resized_images):
                    y = i * target_height
                    grid_image.paste(img, (0, y))
                    img.close()
                
                print(f"Created vertical grid: {grid_width}x{grid_height} ({len(resized_images)} rows x 1 column)")
            
            # Save grid image
            grid_image.save(output_path, quality=95)
            grid_image.close()
            print(f"Saved grid to: {output_path}")
            
        except Exception as e:
            print(f"Error creating single-view grid: {e}")
            import traceback
            traceback.print_exc()
    
    def _create_single_row_grid(self, image_paths: List[str], output_path: str) -> None:
        """Combine multiple images into a single horizontal row grid."""
        try:
            if not image_paths:
                print("Error: No images provided for single-row grid")
                return
            
            # Load all images and track their sizes
            images = []
            sizes = []
            
            for img_path in image_paths:
                if os.path.exists(img_path):
                    img = Image.open(img_path)
                    images.append(img)
                    sizes.append(img.size)
                    print(f"Loaded image: {os.path.basename(img_path)} {img.size}")
                else:
                    print(f"Warning: Image not found: {img_path}")
                    # Create placeholder
                    img = Image.new('RGB', (1024, 800), color='white')
                    images.append(img)
                    sizes.append((1024, 800))
            
            if not images:
                print("Error: No valid images found for single-row grid")
                return
            
            # Use consistent height (max height) and preserve aspect ratios
            max_height = max(size[1] for size in sizes)
            
            # Resize images to same height while preserving aspect ratio
            resized_images = []
            total_width = 0
            
            for img in images:
                # Calculate new width maintaining aspect ratio
                aspect_ratio = img.size[0] / img.size[1]
                new_width = int(max_height * aspect_ratio)
                
                if img.size != (new_width, max_height):
                    img_resized = img.resize((new_width, max_height), Image.Resampling.LANCZOS)
                    resized_images.append(img_resized)
                    img.close()  # Close original
                else:
                    resized_images.append(img)
                
                total_width += new_width
            
            # Create grid image (single row)
            grid_image = Image.new('RGB', (total_width, max_height), color='white')
            
            # Paste images side by side
            x_offset = 0
            for img in resized_images:
                grid_image.paste(img, (x_offset, 0))
                x_offset += img.size[0]
            
            # Save grid image
            grid_image.save(output_path, quality=95)
            print(f"Created single-row grid ({total_width}x{max_height}) with {len(images)} images: {output_path}")
            
            # Clean up
            for img in resized_images:
                img.close()
                
        except Exception as e:
            print(f"Error creating single-row grid: {e}")
            import traceback
            traceback.print_exc()

    def _select_evenly_spaced_tracts(self, tract_list: List[str], max_tracts: int = 10, 
                                   values: Optional[List[float]] = None, values_column_name: Optional[str] = None) -> List[str]:
        """Select evenly spaced tracts from a list, up to max_tracts.
        
        Parameters:
        -----------
        tract_list : List[str]
            List of tract names (should be in sorted order)
        max_tracts : int
            Maximum number of tracts to select
        values : Optional[List[float]]
            Corresponding values for each tract (for display purposes)
        values_column_name : Optional[str]
            Name of the values column (for display purposes)
        """
        if len(tract_list) <= max_tracts:
            if values and values_column_name:
                print(f"Using all {len(tract_list)} tracts (≤ max_tracts={max_tracts})")
                for tract, value in zip(tract_list, values):
                    print(f"  {tract}: {values_column_name} = {value:.3f}")
            return tract_list
        
        # Calculate step size for even spacing
        step = len(tract_list) / max_tracts
        selected_indices = [int(i * step) for i in range(max_tracts)]
        
        # Ensure we don't exceed list bounds
        selected_indices = [min(idx, len(tract_list) - 1) for idx in selected_indices]
        
        selected_tracts = [tract_list[idx] for idx in selected_indices]
        
        # Print selected tracts with their values if provided
        if values and values_column_name:
            print(f"Selected {len(selected_tracts)} evenly spaced tracts from {len(tract_list)} total (step size: {step:.2f}):")
            for i, idx in enumerate(selected_indices):
                tract_name = selected_tracts[i]
                tract_value = values[idx]
                print(f"  {i+1:2d}. {tract_name}: {values_column_name} = {tract_value:.3f}")
        
        return selected_tracts

  
    def _calculate_optimal_grid_dimensions(self, n_items: int) -> Tuple[int, int]:
        """Calculate optimal grid dimensions (rows, cols) for n_items to create a balanced grid."""
        if n_items <= 0:
            return (0, 0)
        if n_items == 1:
            return (1, 1)
        
        # For small numbers, prefer single row
        if n_items <= 6:
            return (1, n_items)
        
        # For larger numbers, try to create a roughly square grid
        # Start with square root and adjust to minimize empty spaces
        import math
        sqrt_n = math.sqrt(n_items)
        
        # Try different row counts around the square root
        best_rows, best_cols = 1, n_items
        min_empty_spaces = n_items
        
        for rows in range(max(1, int(sqrt_n) - 1), int(sqrt_n) + 3):
            cols = math.ceil(n_items / rows)
            empty_spaces = (rows * cols) - n_items
            
            # Prefer configurations with fewer empty spaces, and if tied, prefer fewer rows
            if empty_spaces < min_empty_spaces or (empty_spaces == min_empty_spaces and rows < best_rows):
                best_rows, best_cols = rows, cols
                min_empty_spaces = empty_spaces
        
        return (best_rows, best_cols)
    
    def _create_gradient_grid(self, image_paths: List[str], output_path: str) -> None:
        """Create a grid layout from gradient images with optimal dimensions."""
        if not image_paths:
            print("No images provided for grid creation")
            return
            
        n_images = len(image_paths)
        rows, cols = self._calculate_optimal_grid_dimensions(n_images)
        
        print(f"Creating {rows}x{cols} grid from {n_images} images")
        
        try:
            from PIL import Image
            
            # Load all images and force them to be exactly 1024x800
            images = []
            target_width, target_height = 1024, 800
            
            for img_path in image_paths:
                if os.path.exists(img_path):
                    img = Image.open(img_path)
                    current_size = img.size
                    
                    if current_size != (target_width, target_height):
                        # Resize to exact target dimensions
                        resized_img = img.resize((target_width, target_height), Image.Resampling.LANCZOS)
                        img.close()
                        images.append(resized_img)
                        print(f"Resized gradient image: {os.path.basename(img_path)} from {current_size} to ({target_width}, {target_height})")
                    else:
                        images.append(img)
                        print(f"Loaded gradient image: {os.path.basename(img_path)} ({img.width}x{img.height})")
                else:
                    print(f"Warning: Image not found: {img_path}")
                    # Create placeholder
                    images.append(Image.new('RGB', (target_width, target_height), color='white'))
            
            if not images:
                print("Error: No valid images found for grid creation")
                return
            
            # All images are now guaranteed to be 1024x800
            img_width, img_height = target_width, target_height
            
            # Create the grid
            grid_width = cols * img_width
            grid_height = rows * img_height
            grid_image = Image.new('RGB', (grid_width, grid_height), color='white')
            
            # Place images in the grid
            for i, img in enumerate(images):
                row = i // cols
                col = i % cols
                x = col * img_width
                y = row * img_height
                grid_image.paste(img, (x, y))
                img.close()  # Clean up memory
            
            # Save the grid
            grid_image.save(output_path)
            grid_image.close()
            
            print(f"Created gradient grid ({grid_width}x{grid_height}) with {rows} rows and {cols} columns using {target_width}x{target_height} images: {output_path}")
            
        except Exception as e:
            print(f"Error creating gradient grid: {e}")
            import traceback
            traceback.print_exc()
    

    # ------------------------------------------------------------
    # Colorbar creation
    # ------------------------------------------------------------

    def _add_colorbar_to_image(self, image_path: str, tract_names: List[str], color_scheme: str,
                              values: Optional[List[float]], converted_color_dict: Optional[Dict[str, tuple]],
                              has_single_color: bool, has_values_column: bool, values_column: Optional[str],
                              has_color_column: bool, color_column: Optional[str], full_dataset_colors: Optional[Dict[str, tuple]] = None,
                              colorbar_title: Optional[str] = None) -> None:
        """Add colorbar or legend to an existing image."""
        if not os.path.exists(image_path):
            print(f"Warning: Image not found for colorbar addition: {image_path}")
            return
        
        try:
            import matplotlib.pyplot as plt
            import matplotlib.colors as mcolors
            from matplotlib.patches import Rectangle
            import numpy as np
            
            # Load original image
            original_img = Image.open(image_path)
            img_width, img_height = original_img.size
            
            # Create matplotlib figure
            fig_height_inches = 8
            fig_width_inches = img_width * fig_height_inches / img_height
            
            fig, (ax_main, ax_colorbar) = plt.subplots(2, 1, 
                                                      figsize=(fig_width_inches, fig_height_inches + 1.5),
                                                      gridspec_kw={'height_ratios': [20, 1], 'hspace': 0.05})
            
            # Display main image
            ax_main.imshow(original_img)
            ax_main.axis('off')
            
            if has_single_color:
                # Create discrete legend for single colors
                self._create_discrete_legend(ax_colorbar, tract_names, converted_color_dict)
            else:
                # Create continuous colorbar using color_column colors or color_scheme
                self._create_continuous_colorbar(ax_colorbar, color_scheme, values, has_values_column, values_column, 
                                                has_color_column, converted_color_dict, color_column, full_dataset_colors, colorbar_title)
            
            # Use subplots_adjust instead of tight_layout: colorbar/legend use set_position(), which is
            # incompatible with tight_layout and triggers a UserWarning
            fig.subplots_adjust(left=0.02, right=0.98, top=0.96, bottom=0.02, hspace=0.05)
            plt.savefig(image_path, dpi=150, bbox_inches='tight', pad_inches=0.1)
            plt.close()
            
            # Close original image
            original_img.close()
            
            print(f"Added colorbar to: {image_path}")
            
        except Exception as e:
            print(f"Error adding colorbar to {image_path}: {e}")
            import traceback
            traceback.print_exc()

    def _create_continuous_colorbar(self, ax, color_scheme: str, 
                                  values: Optional[List[float]], has_values_column: bool, values_column: Optional[str],
                                  has_color_column: bool = False, converted_color_dict: Optional[Dict[str, tuple]] = None,
                                  color_column: Optional[str] = None, full_dataset_colors: Optional[Dict[str, tuple]] = None,
                                  colorbar_title: Optional[str] = None) -> None:
        """Create a continuous colorbar for color schemes, values, or color_column."""
        import matplotlib.pyplot as plt
        import matplotlib.colors as mcolors
        import numpy as np
        
        # Get colormap
        if has_color_column and full_dataset_colors:
            # Create custom colormap from FULL dataset colors (not just subset)
            colors = []
            for tract_name, color_tuple in full_dataset_colors.items():
                # Extract RGB (ignore alpha if present)
                rgb = color_tuple[:3]
                colors.append(rgb)
            
            if colors:
                # Create custom colormap from the full dataset colors
                cmap = mcolors.ListedColormap(colors)
                print(f"Created custom colormap from {len(colors)} colors (full dataset) in {color_column}")
            else:
                # Fallback to color_scheme if no colors found
                if color_scheme == 'fds' and HAS_TM_UTILS:
                    cmap = fds_cmap
                else:
                    cmap = plt.cm.get_cmap(color_scheme)
        elif has_color_column and converted_color_dict:
            # Fallback: use subset colors if full dataset not available
            colors = []
            for tract, color_tuple in converted_color_dict.items():
                # Extract RGB (ignore alpha if present)
                rgb = color_tuple[:3]
                colors.append(rgb)
            
            if colors:
                # Create custom colormap from the provided colors
                cmap = mcolors.ListedColormap(colors)
                print(f"Created custom colormap from {len(colors)} colors (subset) in {color_column}")
            else:
                # Fallback to color_scheme if no colors found
                if color_scheme == 'fds' and HAS_TM_UTILS:
                    cmap = fds_cmap
                else:
                    cmap = plt.cm.get_cmap(color_scheme)
        elif color_scheme == 'fds' and HAS_TM_UTILS:
            cmap = fds_cmap
        else:
            cmap = plt.cm.get_cmap(color_scheme)
        
        # Determine value range
        if values is not None and len(values) > 0:
            vmin, vmax = min(values), max(values)
            if vmin == vmax:  # Handle case where all values are the same
                vmin, vmax = vmin - 0.5, vmax + 0.5
        else:
            vmin, vmax = 0, 1
        
        # Create colorbar
        norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        
        cbar = plt.colorbar(sm, cax=ax, orientation='horizontal')
        
        # Remove black edge around colorbar
        cbar.outline.set_visible(False)
        
        # Make colorbar fixed width and center it (consistent size regardless of image width)
        pos = ax.get_position()
        # Fixed width of 0.4 in figure coordinates (0.0 to 1.0 scale)
        # This ensures colorbar appears same size for narrow and wide grids
        fixed_width = 0.4
        new_left = pos.x0 + (pos.width - fixed_width) / 2
        ax.set_position([new_left, pos.y0, fixed_width, pos.height])
        
        # Set label (custom title overrides auto-derived label)
        if colorbar_title is not None:
            label = colorbar_title
        elif has_values_column and values_column:
            # Format column name for display (remove underscores)
            label = f"{values_column.replace('_', ' ').title()}"
        elif has_color_column and color_column:
            # Just color column, no values
            label = color_column.replace('_', ' ').title()
        else:
            label = color_scheme.capitalize()
        
        cbar.set_label(label, fontsize=16)
        cbar.ax.tick_params(labelsize=16)

    def _create_discrete_legend(self, ax, tract_names: List[str], 
                               converted_color_dict: Optional[Dict[str, tuple]]) -> None:
        """Create a discrete legend for single colors."""
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
        
        # Get display names (from abbreviations or inferred short names)
        full_names = self._get_tract_full_names(tract_names)
        
        if not converted_color_dict:
            print("Warning: No color dictionary provided for discrete legend")
            ax.axis('off')
            return
        
        # Clear the axis
        ax.clear()
        ax.axis('off')
        
        # Create legend patches
        patches = []
        labels = []
        
        for i, tract in enumerate(tract_names):
            if tract in converted_color_dict:
                color_rgb = converted_color_dict[tract][:3]  # Remove alpha if present
                patches.append(Rectangle((0, 0), 1, 1, facecolor=color_rgb))
                
                # Use full name if available, otherwise use tract name
                display_name = full_names.get(tract, tract)
                labels.append(display_name)
        
        if patches:
            # Create horizontal legend
            legend = ax.legend(patches, labels, loc='center', ncol=min(len(patches), 4), 
                             frameon=True, fontsize=16, title="Tracts", title_fontsize=16)
            legend.get_title().set_fontweight('bold')
            
            # Make legend area fixed width and center it (consistent with colorbar)
            pos = ax.get_position()
            # Fixed width of 0.4 in figure coordinates (0.0 to 1.0 scale)
            # This ensures legend appears same size as colorbar for visual consistency
            fixed_width = 0.4
            new_left = pos.x0 + (pos.width - fixed_width) / 2
            ax.set_position([new_left, pos.y0, fixed_width, pos.height])
        
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)

    def _get_tract_full_names(self, tract_names: List[str]) -> Dict[str, str]:
        """Get display names for tracts (from abbreviations file or inferred short names)."""
        full_names = {}
        if self.abbreviations.empty:
            for tract in tract_names:
                full_names[tract] = self._canonical_to_short_name(tract).replace('_', ' ')
            return full_names
        if 'Full_Name_capitalized' not in self.abbreviations.columns:
            for tract in tract_names:
                full_names[tract] = self._canonical_to_short_name(tract).replace('_', ' ')
            return full_names
        try:
            for tract in tract_names:
                matching_rows = self.abbreviations[self.abbreviations['HCPYA_1065'] == tract]
                if not matching_rows.empty:
                    full_name = matching_rows.iloc[0]['Full_Name_capitalized']
                    if pd.notna(full_name) and full_name.strip():
                        full_names[tract] = full_name.strip()
                    else:
                        full_names[tract] = tract
                else:
                    for col in self.abbreviations.columns:
                        if col.startswith(('HCPYA', 'HCP', 'Tract')):
                            matching_rows = self.abbreviations[self.abbreviations[col] == tract]
                            if not matching_rows.empty:
                                full_name = matching_rows.iloc[0]['Full_Name_capitalized']
                                if pd.notna(full_name) and full_name.strip():
                                    full_names[tract] = full_name.strip()
                                    break
                    if tract not in full_names:
                        full_names[tract] = tract
        except Exception as e:
            print(f"Error getting full tract names: {e}")
            for tract in tract_names:
                full_names[tract] = tract
        return full_names
