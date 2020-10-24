from pathlib import Path
import pandas as pd
import argparse


if __name__ == '__main__':

    directory = Path('records')
    directory.mkdir(exist_ok=True, parents=True)

    for file in Path('./').glob('*.csv'):
        df = pd.read_csv(file).to_parquet(directory / f'{file.stem}.parquet')
