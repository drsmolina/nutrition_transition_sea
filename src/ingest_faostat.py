import argparse
import yaml
import pandas as pd
from pathlib import Path
import pycountry

def name_to_iso3(name: str) -> str:
    overrides = {"Viet Nam": "VNM", "Lao People's Democratic Republic": "LAO"}
    if name in overrides:
        return overrides[name]
    try:
        return pycountry.countries.lookup(name).alpha_3
    except Exception:
        return None

def main(config_path: str):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    raw_dir = Path(config["paths"]["raw"])
    tidy_dir = Path(config["paths"]["tidy"])
    tidy_dir.mkdir(parents=True, exist_ok=True)

    # find the FAOSTAT FBS file (.csv or .csv.gz)
    files = list(raw_dir.glob("faostat_fbs*.csv.gz")) + list(raw_dir.glob("faostat_fbs*.csv"))
    if not files:
        raise FileNotFoundError("FAOSTAT file not found in data_raw directory")
    fbs_path = files[0]

    # read the FAOSTAT dataset
    df = pd.read_csv(fbs_path, compression="infer")

    # filter to Food supply (kcal/capita/day)
    mask = df["element"].str.contains("Food supply", case=False, na=False) & df["unit"].str.contains("kcal", case=False, na=False)
    df = df[mask].copy()

    # load category mapping
    cat_map = pd.read_csv(Path("fao_category_map.csv"))

    # merge mapping on item name
    df = df.merge(cat_map, left_on="item", right_on="fao_group", how="left")
    df = df.dropna(subset=["category"])

    # convert country names to ISO3 codes
    df["iso3"] = df["area"].apply(name_to_iso3)
    df = df.dropna(subset=["iso3"])

    # aggregate energy supply by iso3, year, category
    grouped = df.groupby(["iso3", "year", "category"])["value"].sum().reset_index()

    # pivot to wide format
    pivot = grouped.pivot_table(index=["iso3", "year"], columns="category", values="value", fill_value=0).reset_index()

    # ensure all categories exist
    categories = ["staples", "meat", "dairy", "fruitveg", "oils", "sugar"]
    for cat in categories:
        if cat not in pivot.columns:
            pivot[cat] = 0.0

    # compute total kcal and category shares
    total = pivot[categories].sum(axis=1)
    for cat in categories:
        pivot[f"share_{cat}"] = pivot[cat] / total

    # save tidy FAOSTAT data
    out_path = tidy_dir / "faostat_tidy.csv"
    pivot.to_csv(out_path, index=False)
    print(f"Saved tidy FAOSTAT data to {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest FAOSTAT data")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    args = parser.parse_args()
    main(args.config)
