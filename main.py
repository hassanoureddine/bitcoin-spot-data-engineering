import os
import pandas as pd
import datetime

from coinapi_rest_v1.restapi import CoinAPIv1
from google.cloud import bigquery

import config



def _collect_data():
    api = CoinAPIv1(config.API_key)
    start_date = datetime.date(2022, 3, 23).isoformat()

    orderbooks_historical_data_btc_usd = api.orderbooks_historical_data('BITSTAMP_SPOT_BTC_USD',
                                                                        {'time_start': start_date})

    for data in orderbooks_historical_data_btc_usd:
        file_index = str(int(datetime.datetime.strptime(data['time_exchange'][:19], '%Y-%m-%dT%H:%M:%S').timestamp()))

        df_asks = pd.DataFrame(data['asks'])
        file_name_asks = './asks_data/' + file_index + '.csv'
        df_asks.to_csv(file_name_asks)

        df_bids = pd.DataFrame(data['bids'])
        ile_name_bids = './bids_data/' + file_index + '.csv'
        df_bids.to_csv(ile_name_bids)

def _load_existing_tables(client):
    tables = client.list_tables(config.bq_dataset_id)
    tables_ids = []
    for table in tables:
        tables_ids.append(table.table_id)
    return tables_ids

def _load_asks_data(client):
    asks_table_id = 'btcspot.btcspot.asks'
    if 'asks' not in _load_existing_tables(client):
        schema_asks = [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("size", "FLOAT", mode="REQUIRED"),
        ]
        table = bigquery.Table(asks_table_id, schema=schema_asks)
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    input_files = [f for f in os.listdir('asks_data') if f.endswith(('.csv', '.CSV'))]

    for file in input_files:
        df = pd.read_csv('asks_data' + '/' + file)
        df['price'] = pd.to_numeric(df['price'])
        df['size'] = pd.to_numeric(df['size'])
        timestamp = int(file[:10])

        row_to_insert = []
        for index, row in df.iterrows():
            row_to_insert = row_to_insert + [
                {u'timestamp': timestamp, u'price': row['price'], u'size': row['size']}
            ]

        errors = client.insert_rows_json(asks_table_id, row_to_insert)
        if errors == []:
            print('New rows have been added to {}'.format(asks_table_id))
        else:
            print('Encountered errors while inserting rows: {errors}')

        #for testing purposes
        #break

def _load_bids_data(client):
    bids_table_id = 'btcspot.btcspot.bids'
    if 'bids' not in _load_existing_tables(client):
        schema_asks = [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("size", "FLOAT", mode="REQUIRED"),
        ]
        table = bigquery.Table(bids_table_id, schema=schema_asks)
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    input_files = [f for f in os.listdir('bids_data') if f.endswith(('.csv', '.CSV'))]

    for file in input_files:
        df = pd.read_csv('bids_data' + '/' + file)
        df['price'] = pd.to_numeric(df['price'])
        df['size'] = pd.to_numeric(df['size'])
        timestamp = int(file[:10])

        row_to_insert = []
        for index, row in df.iterrows():
            row_to_insert = row_to_insert + [
                {u'timestamp': timestamp, u'price': row['price'], u'size': row['size']}
            ]

        errors = client.insert_rows_json(bids_table_id, row_to_insert)
        if errors == []:
            print('New rows have been added to {}'.format(bids_table_id))
        else:
            print('Encountered errors while inserting rows: {errors}')

        #for testing purposes
        #break

def _load_data():
    credentials_path = 'pythonbq-privateKey.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    client = bigquery.Client()

    _load_asks_data(client)
    _load_bids_data(client)


if __name__ == '__main__':
    _collect_data()

    _load_data()
