# scripts/config.py
from pathlib import Path

# locate the root dir of project (where README.md or pyproject.toml lives)
project_root = Path(__file__).resolve().parents[1]

# define paths
raw_data_dir = project_root / "data" / "raw"
cleaned_data_dir = project_root / "data" / "cleaned"
notebooks_dir = project_root / "notebooks"
dbt_dir = project_root / "dbt"

# export any path you need
RAW_DATA_DIR = raw_data_dir
CLEANED_DATA_DIR = cleaned_data_dir
NOTEBOOKS_DIR = notebooks_dir
DBT_DIR = dbt_dir
