import os
from google.oauth2 import service_account
import pandas_gbq
import pendulum
import pytz
from datetime import datetime

import env

credentials = service_account.Credentials.from_service_account_file(os.environ['KEYFILE'])


class PriceData:
    def __init__(self, symbol, source='bigquery', tz='UTC'):
        self.symbol = symbol
        self.data = None
        self.source = source
        self.tz = pytz.timezone(tz)

    def fetch_query(self, sql, index_col='time'):
        self.data = pandas_gbq.read_gbq(sql, index_col=index_col, project_id=os.environ['BQ_PROJECT'],
                                        credentials=credentials)
        self.data.sort_index(inplace=True)
        self._print_data_shape()

    def fetch_table(self, table_name, index_col='time'):
        query = "select * from {TABLE}".format(TABLE=table_name)
        self.data = pandas_gbq.read_gbq(query, index_col=index_col, project_id=os.environ['BQ_PROJECT'],
                                        credentials=credentials)
        self.data.sort_index(inplace=True)
        self._print_data_shape()

    # return a slice of data
    def slice(self, start, end, period=None):

        end_parsed, start_parsed = self._parse_datetime_strings(start, end)
        start_datetime = self._pendulum_to_datetime(start_parsed)
        end_datetime = self._pendulum_to_datetime(end_parsed)

        time_slice = self.data[start_datetime: end_datetime]
        if period is not None:
            return time_slice[time_slice.period == period]
        else:
            return time_slice

    def _parse_datetime_strings(self, start, end):
        start_parsed = pendulum.parse(start)
        end_parsed = pendulum.parse(end)
        return end_parsed, start_parsed

    # return different aspects of price
    def open(self):
        self._data_exists()
        return self.data.loc[:, 'open']

    def high(self):
        self._data_exists()
        return self.data.loc[:, 'high']

    def low(self):
        self._data_exists()
        return self.data.loc[:, 'low']

    def close(self):
        self._data_exists()
        return self.data.loc[:, 'close']

    def _print_data_shape(self):
        print(self.data.shape)

    def _data_exists(self):
        if self.data is None:
            raise ValueError("no data! get it first")

    def _pendulum_to_datetime(self, pendulum_obj):
        return datetime(pendulum_obj.year, pendulum_obj.month, pendulum_obj.day, pendulum_obj.minute,
                        pendulum_obj.second, pendulum_obj.microsecond, tzinfo=self.tz)
