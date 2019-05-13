import pandas
from google.oauth2 import service_account
import pandas_gbq
import os
import env

credentials = service_account.Credentials.from_service_account_file(os.environ['KEYFILE'])


class PriceData:
    def __init__(self, symbol, source='bigquery'):
        self.symbol = symbol
        self.data = None
        self.source = source

    def fetch_query(self, sql):
        self.data = pandas_gbq.read_gbq(sql, project_id=os.environ['BQ_PROJECT'], credentials=credentials)
        self._print_data_shape()


    def fetch_table(self, table_name):
        query = "select * from {TABLE}".format(TABLE=table_name)
        self.data = pandas_gbq.read_gbq(query, project_id=os.environ['BQ_PROJECT'], credentials=credentials)
        self._print_data_shape()

    # return a slice of data
    def slice(self):
        pass

    # return different aspects of price
    def open(self):
        pass

    def high(self):
        pass

    def low(self):
        pass

    def close(self):
        pass


    def _print_data_shape(self):
        print(self.data.shape)

