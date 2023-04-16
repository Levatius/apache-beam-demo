import datetime as dt
import logging
import unittest
import pytz

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

import traditional_pipeline


class TraditionalPipelineTest(unittest.TestCase):
    def setUp(self):
        self.filter_amount = 20
        self.filter_year = dt.datetime(year=2010, month=1, day=1, tzinfo=pytz.UTC)

    def test_composite_transform(self):
        with TestPipeline() as p:
            with open('data/test_input.csv') as f:
                input_data = f.read().splitlines()[1:]

            generated_results = (
                p
                | beam.Create(input_data)
                | 'Apply Transform' >> traditional_pipeline.CompositeTransform(self.filter_amount, self.filter_year)
            )

            with open('data/test_expected.csv') as f:
                expected_results = f.read().splitlines()[1:]

            assert_that(generated_results, equal_to(expected_results))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
