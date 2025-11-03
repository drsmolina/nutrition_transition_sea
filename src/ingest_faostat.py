import argparse
import yaml
import pandas as pd
from pathlib import Path
import pycountry


def name_to_iso3(name: str) -> str:
    """Convert country name to ISO3 code with overrides."""
    overrides = {
        "Viet Nam": "VNM",
        "Lao People's Democratic Republic": "LAO",
    }
    if name in overrides:
        return overrides[name]
    try:
        return pycountry.countries.lookup(name).alpha_3
    except Exception:
        return None


def main(config_path: str):
    """Ingest FAOSTAT Food Balance Sheets data and compute dietary shares."""
    # load config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    raw_dir = Path(config['paths']['raw'])
    tidy_dir = Path(config['paths']['tidy'])
    tidy_dir.mkdir(parents=True, exist_ok=True)

    # locate FAOSTAT file (CSV or compressed)
    files = list(raw_dir.glob('faostat_fbs*.csv.gz')) + list(raw_dir.glob('faostat_fbs*.csv'))
    if not files:
        raise FileNotFoundError('FAOSTAT file not found in data_raw directory')
    fbs_path = files[0]

    # read FAOSTAT dataset
    df = pd.read_csv(fbs_path, compression='infer')

    # filter to food supply (kcal/cap/day) element (case-insensitive)
    mask = df['element'].str.contains('food supply', case=False, na=False) & df['unit'].str.contains('kcal', case=False, na=False)
    df = df[mask].copy()

    # read category mapping CSV (assumed to be in repo root)
    map_path = Path('fao_category_map.csv')
    cat_map = pd.read_csv(map_path)

    # merge mapping on item (fao_group)
    df = df.merge(cat_map, left_on='item', right_on='fao_group', how='left')
    df = df.dropna(subset=['category'])

    # convert area name to iso3
    df['iso3'] = df['area'].apply(name_to_iso3)
    df = df.dropna(subset=['iso3'])

    # aggregate kcal per iso3, year, category
    grouped = df.groupby(['iso3', 'year', 'category'])['value'].sum().reset_index()

    # pivot to wide format
    pivot = grouped.pivot_table(index=['iso3', 'year'], columns='category', values='value', fill_value=0).reset_index()

    # ensure all expected categories exist
    categories = ['staples', 'meat', 'dairy', 'fruitveg', 'oils', 'sugar']
    for cat in categories:
        if cat not in pivot.columns:
            pivot[cat] = 0.0

    # compute total kcal and shares
    total = pivot[categories].sum(axis=1)
    for cat in categories:
        pivot[f'share_{cat}'] = pivot[cat] / total

    # save tidy FAOSTAT data
    out_path = tidy_dir / 'faostat_tidy.csv'
    pivot.to_csv(out_path, index=False)
    print(f'Saved tidy FAOSTAT data to {out_path}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest FAOSTAT data')
    parser.add_argument('--config', required=True, help='Path to config YAML file')
    args = parser.parse_args()
    main(args.config)
