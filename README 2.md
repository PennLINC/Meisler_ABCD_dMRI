# ABCD dMRI Harmonization Project

## Required R Packages

Install required R packages with:

```r
install.packages(c(
  "dplyr",
  "readr",
  "stringr",
  "tidyr",
  "arrow",
  "fs",
  "purrr",
  "jsonlite"
))
```

### Package Descriptions

- **dplyr**: Data manipulation and transformation
- **readr**: Reading CSV/TSV files
- **stringr**: String operations and pattern matching
- **tidyr**: Data reshaping (pivot_wider, etc.)
- **arrow**: Reading/writing Parquet files
- **fs**: File system operations
- **purrr**: Functional programming tools
- **jsonlite**: Reading JSON configuration files

## Required Python Packages

_(To be added as Python scripts are developed)_

## Configuration

Project configuration is stored in `config.json`. Set the `CONFIG_PATH` environment variable to point to this file:

```bash
export CONFIG_PATH=/path/to/config.json
```
