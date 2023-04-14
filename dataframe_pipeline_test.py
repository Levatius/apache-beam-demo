import datetime as dt
import pandas as pd
import pytz
import unittest

import dataframe_pipeline


class DataframePipelineTest(unittest.TestCase):
    def setUp(self):
        self.input_df = pd.read_csv('data/test_input.csv', parse_dates=['timestamp'])
        expected_df = pd.read_csv('data/test_expected.csv', parse_dates=['date'])
        # parse_dates produces values with dt.datetime, convert to dt.date:
        expected_df['date'] = expected_df['date'].dt.date
        self.expected_series = expected_df.set_index('date').squeeze()
        self.filter_amount = 20
        self.filter_year = dt.datetime(year=2010, month=1, day=1, tzinfo=pytz.UTC)

    def test_transform(self):
        generated_series = dataframe_pipeline.run_transform(self.input_df, self.filter_amount, self.filter_year)
        pd.testing.assert_series_equal(generated_series, self.expected_series)
