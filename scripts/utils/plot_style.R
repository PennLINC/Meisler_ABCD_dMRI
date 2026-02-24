#!/usr/bin/env Rscript

# ------------------------------------------------------------------------------
# plot_style.R -- Shared plotting utilities for figure and analysis scripts
# ------------------------------------------------------------------------------
# - Colors: metric_colors, vendor_colors, bundle_colors, bundle_color_pretty
# - Layout: nature_figure_specs, mm_to_in, get_figure_dims, pt_for_export
# - Theme: get_plot_style, make_theme_pub, add_panel_label
# - Config: find_config_path, get_project_config (requires jsonlite)
# - Export: get_export_font_family, save_plot_outputs (requires fs)
# - Bundle labels: bundle_category_label_map, category_label_map (alias),
#   bundle_category_order_pretty, category_order_pretty (alias)
# - Core microstructural metrics: metrics_keep, metric_labels, metric_order
#   (used by Figure 3, 4, 5, 6)
# ------------------------------------------------------------------------------

# Five core microstructural metrics and display labels (shared by Figure 3–6).
metrics_keep <- c("DKI_mkt", "NODDI_icvf", "MAPMRI_rtop", "GQI_fa", "GQI_md")
metric_labels <- c(
  "DKI_mkt" = "MKT",
  "NODDI_icvf" = "ICVF",
  "MAPMRI_rtop" = "RTOP",
  "GQI_fa" = "FA",
  "GQI_md" = "MD"
)
# Display order for factor levels (same order as metrics_keep, as display names).
metric_order <- unname(metric_labels[metrics_keep])

# Fixed color dictionaries to stay consistent across all figures.
metric_colors <- c(
  "MKT" = "#A65628",
  "FA" = "#6A3D9A",
  "ICVF" = "#4D4D4D",
  "MD" = "#E7298A",
  "RTOP" = "#66A61E"
)

vendor_colors <- c(
  "Siemens" = "#0072B2",
  "GE" = "#D55E00",
  "Philips" = "#009E73"
)

bundle_colors <- c(
  "Association"          = "#4E79A7",
  "ProjectionBrainstem"  = "#F28E2B",
  "ProjectionBasalGanglia" = "#59A14F",
  "Cerebellum"           = "#E15759",
  "Commissure"           = "#B07AA1"
)

# Journal figure size guidance for this project.
nature_figure_specs <- list(
  one_column_width_mm = 88,
  two_column_width_mm = 180,
  one_column_max_height_mm = 180,
  two_column_max_height_mm = 210,
  min_text_pt = 5,
  max_text_pt = 7,
  preferred_font_family = "Arial"
)

mm_to_in <- function(mm) {
  mm / 25.4
}

in_to_mm <- function(inches) {
  inches * 25.4
}

get_figure_dims <- function(layout = c("one_column", "two_column"), height_mm = NULL) {
  layout <- match.arg(layout)

  if (layout == "one_column") {
    width_mm <- nature_figure_specs$one_column_width_mm
    max_height_mm <- nature_figure_specs$one_column_max_height_mm
  } else {
    width_mm <- nature_figure_specs$two_column_width_mm
    max_height_mm <- nature_figure_specs$two_column_max_height_mm
  }

  if (is.null(height_mm)) {
    height_mm <- max_height_mm
  }
  if (!is.numeric(height_mm) || length(height_mm) != 1 || !is.finite(height_mm) || height_mm <= 0) {
    stop("height_mm must be one positive finite number.")
  }
  if (height_mm > max_height_mm) {
    stop(sprintf("height_mm (%.1f) exceeds max height %.1f mm for %s layout.", height_mm, max_height_mm, layout))
  }

  list(
    layout = layout,
    width_mm = width_mm,
    height_mm = height_mm,
    max_height_mm = max_height_mm,
    width_in = mm_to_in(width_mm),
    height_in = mm_to_in(height_mm)
  )
}

# Scale factor to preserve a target final text size after resizing in layout software.
compute_text_scale <- function(export_width_in, placed_width_in = export_width_in) {
  if (!is.finite(export_width_in) || !is.finite(placed_width_in) ||
      export_width_in <= 0 || placed_width_in <= 0) {
    stop("export_width_in and placed_width_in must be positive finite numbers.")
  }
  export_width_in / placed_width_in
}

validate_final_text_pt <- function(final_pt, strict = FALSE) {
  ok <- is.finite(final_pt) &&
    final_pt >= nature_figure_specs$min_text_pt &&
    final_pt <= nature_figure_specs$max_text_pt

  if (!ok) {
    msg <- sprintf(
      "Final text size %.2f pt is outside recommended %.1f-%.1f pt range.",
      final_pt,
      nature_figure_specs$min_text_pt,
      nature_figure_specs$max_text_pt
    )
    if (strict) stop(msg) else warning(msg, call. = FALSE)
  }
  invisible(ok)
}

pt_for_export <- function(
    final_pt,
    export_width_in,
    placed_width_in = export_width_in,
    strict = FALSE) {
  validate_final_text_pt(final_pt, strict = strict)
  final_pt * compute_text_scale(export_width_in, placed_width_in)
}

get_plot_style <- function(config = list()) {
  cfg <- config$plot_style
  if (is.null(cfg)) cfg <- list()

  defaults <- list(
    font_family = nature_figure_specs$preferred_font_family,
    axis_line_color = "black",
    axis_line_width = 0.4,
    axis_tick_color = "black",
    axis_text_color = "black",
    axis_tick_width = 0.4,
    axis_tick_length_pt = 2.2,
    panel_background_fill = "white",
    plot_background_fill = "white",
    strip_text_face = "bold"
  )

  utils::modifyList(defaults, cfg)
}

make_theme_pub <- function(
    style,
    size_scale = 1,
    axis_title_pt = 6,
    axis_text_pt = 5.5,
    plot_title_pt = 6.5,
    legend_title_pt = 6,
    legend_text_pt = 5.5,
    legend_position = "none",
    base_size_pt = 6) {
  ggplot2::theme_classic(
    base_family = style$font_family,
    base_size = base_size_pt * size_scale
  ) +
    ggplot2::theme(
      panel.grid.major = ggplot2::element_blank(),
      panel.grid.minor = ggplot2::element_blank(),
      axis.title = ggplot2::element_text(size = axis_title_pt * size_scale),
      axis.text = ggplot2::element_text(
        size = axis_text_pt * size_scale,
        color = style$axis_text_color
      ),
      plot.title = ggplot2::element_text(
        size = plot_title_pt * size_scale,
        face = "bold",
        hjust = 0.5
      ),
      legend.title = ggplot2::element_text(
        size = legend_title_pt * size_scale,
        face = "bold"
      ),
      legend.text = ggplot2::element_text(size = legend_text_pt * size_scale),
      legend.position = legend_position,
      axis.line = ggplot2::element_line(
        linewidth = style$axis_line_width,
        color = style$axis_line_color
      ),
      axis.ticks = ggplot2::element_line(
        color = style$axis_tick_color,
        linewidth = style$axis_tick_width
      ),
      axis.ticks.length = grid::unit(style$axis_tick_length_pt, "pt"),
      strip.text = ggplot2::element_text(face = style$strip_text_face),
      panel.background = ggplot2::element_rect(fill = style$panel_background_fill, color = NA),
      plot.background = ggplot2::element_rect(fill = style$plot_background_fill, color = NA)
    )
}

# Adds a bold "a)", "b)", etc. panel tag in the top-left corner.
add_panel_label <- function(
    plot,
    label,
    final_pt = 7,
    export_width_in = NULL,
    placed_width_in = export_width_in,
    x = 0.01,
    y = 0.99,
    family = nature_figure_specs$preferred_font_family) {
  if (is.null(export_width_in)) {
    export_text_pt <- final_pt
  } else {
    export_text_pt <- pt_for_export(
      final_pt = final_pt,
      export_width_in = export_width_in,
      placed_width_in = placed_width_in
    )
  }

  plot +
    ggplot2::labs(tag = paste0(label, ")")) +
    ggplot2::theme(
      plot.tag = ggplot2::element_text(
        face = "bold",
        family = family,
        size = export_text_pt
      ),
      plot.tag.position = c(x, y)
    )
}

# ------------------------------------------------------------------------------
# Project config (requires jsonlite when using get_project_config)
# ------------------------------------------------------------------------------

#' Find config.json path: CONFIG_PATH env, then . / .. / ../..
find_config_path <- function() {
  candidates <- c(
    Sys.getenv("CONFIG_PATH", unset = ""),
    file.path(".", "config.json"),
    file.path("..", "config.json"),
    file.path("..", "..", "config.json")
  )
  candidates <- unique(candidates[nzchar(candidates)])
  paths <- normalizePath(candidates, winslash = "/", mustWork = FALSE)
  for (p in paths) {
    if (file.exists(p)) return(p)
  }
  NA_character_
}

#' Load config and project root. Requires jsonlite. Returns list(config, project_root, config_path).
get_project_config <- function() {
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("get_project_config requires jsonlite. Install with install.packages(\"jsonlite\").")
  }
  config_path <- find_config_path()
  if (is.na(config_path) || !nzchar(config_path)) {
    stop("Could not locate config.json. Set CONFIG_PATH or run from project tree.")
  }
  config <- jsonlite::fromJSON(config_path)
  project_root <- normalizePath(dirname(config_path), winslash = "/", mustWork = TRUE)
  list(config = config, project_root = project_root, config_path = config_path)
}

# ------------------------------------------------------------------------------
# Export font selection (Arial preferred, with cairo render check)
# ------------------------------------------------------------------------------

#' Font family for PDF export. Tries Arial variants, then cairo render check; falls back to "sans".
get_export_font_family <- function() {
  candidates <- c("Arial", "Arial MT", "ArialMT")
  font_use <- "sans"
  if (requireNamespace("systemfonts", quietly = TRUE)) {
    installed <- unique(systemfonts::system_fonts()$family)
    match_fam <- candidates[candidates %in% installed][1]
    if (!is.na(match_fam) && nzchar(match_fam)) font_use <- match_fam
  }
  # Check which family actually renders with cairo_pdf
  can_render <- function(fam) {
    tf <- tempfile(fileext = ".pdf")
    ok <- tryCatch({
      grDevices::cairo_pdf(tf, family = fam, width = 2, height = 2)
      grDevices::dev.off()
      TRUE
    }, error = function(e) {
      try(grDevices::dev.off(), silent = TRUE)
      FALSE
    })
    if (file.exists(tf)) unlink(tf)
    ok
  }
  to_check <- unique(c(font_use, candidates))
  to_check <- to_check[to_check != "sans"]
  for (f in to_check) {
    if (can_render(f)) {
      return(f)
    }
  }
  "sans"
}

# ------------------------------------------------------------------------------
# Save plot to PNG + PDF (for figure scripts). Requires fs.
# ------------------------------------------------------------------------------

#' Save a ggplot to PNG and PDF under out_dir. Uses cairo_pdf with optional font; falls back to sans on failure.
save_plot_outputs <- function(plot_obj,
                              stub,
                              out_dir,
                              width_in,
                              height_in,
                              pdf_family = NULL,
                              allow_sans_fallback = TRUE,
                              bg = "white") {
  if (!requireNamespace("fs", quietly = TRUE)) {
    stop("save_plot_outputs requires fs. Install with install.packages(\"fs\").")
  }
  if (is.null(pdf_family)) pdf_family <- get_export_font_family()
  pdf_path <- fs::path(out_dir, paste0(stub, ".pdf"))
  png_path <- fs::path(out_dir, paste0(stub, ".png"))
  ggplot2::ggsave(
    filename = png_path,
    plot = plot_obj,
    width = width_in,
    height = height_in,
    units = "in",
    dpi = 600,
    bg = bg
  )
  pdf_ok <- FALSE
  err <- NULL
  tryCatch({
    ggplot2::ggsave(
      filename = pdf_path,
      plot = plot_obj,
      width = width_in,
      height = height_in,
      units = "in",
      device = function(...) grDevices::cairo_pdf(..., family = pdf_family),
      bg = bg
    )
    pdf_ok <- TRUE
  }, error = function(e) { err <<- e })
  if (!pdf_ok && allow_sans_fallback && pdf_family != "sans") {
    tryCatch({
      ggplot2::ggsave(
        filename = pdf_path,
        plot = plot_obj,
        width = width_in,
        height = height_in,
        units = "in",
        device = function(...) grDevices::cairo_pdf(..., family = "sans"),
        bg = bg
      )
      pdf_ok <- TRUE
    }, error = function(e) {})
  }
  if (!pdf_ok) {
    stop("PDF export failed for ", stub, ": ", conditionMessage(err))
  }
  message("[SAVED] ", pdf_path)
  message("[SAVED] ", png_path)
  invisible(list(pdf = pdf_path, png = png_path))
}

# ------------------------------------------------------------------------------
# Bundle category labels and order (shared across Figure 4, 5, 6)
# ------------------------------------------------------------------------------

bundle_category_label_map <- c(
  "Association" = "Association",
  "ProjectionBasalGanglia" = "Projection (Basal Ganglia)",
  "ProjectionBrainstem" = "Projection (Brainstem)",
  "Cerebellum" = "Cerebellum",
  "Commissure" = "Corpus Callosum"
)

bundle_category_order_pretty <- c(
  "Association",
  "Projection (Basal Ganglia)",
  "Projection (Brainstem)",
  "Cerebellum",
  "Corpus Callosum"
)

# Colors for bundle categories using pretty labels (for plots)
bundle_color_pretty <- c(
  "Association" = bundle_colors[["Association"]],
  "Projection (Basal Ganglia)" = bundle_colors[["ProjectionBasalGanglia"]],
  "Projection (Brainstem)" = bundle_colors[["ProjectionBrainstem"]],
  "Cerebellum" = bundle_colors[["Cerebellum"]],
  "Corpus Callosum" = bundle_colors[["Commissure"]]
)

# Aliases for notebooks that use these names
category_label_map <- bundle_category_label_map
category_order_pretty <- bundle_category_order_pretty
