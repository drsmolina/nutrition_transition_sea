import argparse
import yaml
import pandas as pd
import numpy as np
from pathlib import Path


def main(config_path: str):
    """Build merged panel dataset from tidy FAOSTAT and World Bank data."""
    # Load configuration
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Paths
    tidy_dir = Path(config["paths"]["tidy"])

    # Read tidy FAOSTAT and World Bank data
    fao_path = tidy_dir / "faostat_tidy.csv"
    wb_path = tidy_dir / "worldbank_tidy.csv"
    fao_df = pd.read_csv(fao_path)
    wb_df = pd.read_csv(wb_path)

    # Merge on iso3 and year
    panel = pd.merge(fao_df, wb_df, on=["iso3", "year"], how="inner")

    # Compute log GDP per capita
    panel["log_gdp_pc"] = np.log(panel["gdp_pc_const"])

    # Compute processed-to-traditional ratio
    denom = panel["staples"] + panel["fruitveg"]
    numer = panel["meat"] + panel["sugar"] + panel["oils"]
    panel["processed_to_traditional_ratio"] = numer / denom

    # Save panel
    out_path = tidy_dir / "panel.csv"
    panel.to_csv(out_path, index=False)
    print(f"Saved panel to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build merged panel dataset")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    args = parser.parse_args()
    main(args.config)
