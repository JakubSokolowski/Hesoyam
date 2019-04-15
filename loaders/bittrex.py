import os
from datetime import datetime
import time
import pandas as pd
from credsmanager import data_path


def load_coinmarketcap_data() -> None:
    df: pd.DataFrame = pd.read_csv(data_path + "coinmarket.csv")
    df.drop(['CoinId', 'Id'], axis=1, inplace=True)
    df.rename(columns={
        'Id': 'id',
        'Symbol': 'symbol',
        'Date': 'date',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume',
        'MarketCap': 'marketCap'
    }, inplace=True)
    print(df.head())
    df['date'] = pd.to_datetime(df['date'])
    print(df.tail())


def load_bittrex_data() -> None:
    path: str = data_path + 'bittrex/'
    master_df: pd.DataFrame = pd.DataFrame()

    for filename in os.listdir(path):
        df: pd.DataFrame = pd.read_csv(path + filename, skiprows=1)
        columns: [] = list(df.columns.values)
        columns = list(map(lambda n: (n.lower().replace(' ', '_')), columns))
        columns[-2] = 'volume_self'
        df.columns = columns
        master_df = pd.concat([master_df, df], ignore_index=True)

    master_df['date'] = master_df['date'].apply(to_utc)
    master_df.to_csv(data_path + 'bittrex_master.csv', index=False)


def to_utc(date_str: str) -> str:
    date_format: str = "%Y-%m-%d %H-%p"
    date = datetime.strptime(date_str, date_format)
    return str(time.mktime(date.timetuple()))
