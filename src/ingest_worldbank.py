import argparse
import yaml
import pandas as pd
from pathlib import Path


def main(config_path: str):
    """Ingest World Bank indicators from a pre-downloaded CSV and write tidy file."""
    # load config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    raw_dir = Path(config['paths']['raw'])
    tidy_dir = Path(config['paths']['tidy'])
    tidy_dir.mkdir(parents=True, exist_ok=True)

    # assume world bank CSV exists in raw_dir with name starting with 'worldbank'
    # you can adjust this logic as needed
    csv_files = list(raw_dir.glob('worldbank*.csv'))
    if not csv_files:
        raise FileNotFoundError('No worldbank CSV file found in data_raw directory')

    wb_path = csv_files[0]
    df = pd.read_csv(wb_path)

    # rename iso3c column to iso3 if present
    if 'iso3c' in df.columns:
        df = df.rename(columns={'iso3c': 'iso3'})

    # ensure year is integer
    if 'year' in df.columns:
        df['year'] = df['year'].astype(int)

    # save tidy dataset
    out_path = tidy_dir / 'worldbank_tidy.csv'
    df.to_csv(out_path, index=False)
    print(f'Saved tidy World Bank data to {out_path}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest World Bank data')
    parser.add_argument('--config', required=True, help='Path to config YAML file')
    args = parser.parse_args()
    main(args.config)
