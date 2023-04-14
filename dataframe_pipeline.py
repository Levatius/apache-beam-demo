import argparse
import datetime as dt
import logging
import pytz

import apache_beam as beam
from apache_beam.dataframe.io import read_csv
from apache_beam.options.pipeline_options import PipelineOptions


# Wanted to make into a Composite Transform, however it is unclear how this is done cleanly with DeferredDataframes
# According to the documentation, Composite Transforms inherit from PTransforms which limits IO to PCollections
# Would have to convert read_csv's DeferredDataframe output to a Schema-Aware PCollection only to convert it back here
def run_transform(df, amount, year):
    df = df[df.transaction_amount > amount]
    df = df[df.timestamp > year]
    df['date'] = df.timestamp.dt.date
    series = df.groupby(df.date).transaction_amount.sum()
    series = series.rename('total_amount')
    return series


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv',
        help='Input CSV to process.')
    parser.add_argument(
        '--amount_filter',
        dest='amount_filter',
        default='20',
        help='Filter transactions to those strictly greater than this amount.'
    )
    parser.add_argument(
        '--year_filter',
        dest='year_filter',
        default='2010',
        help='Filter transactions to those made during or after this year.'
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline = beam.Pipeline(options=pipeline_options)

    amount = int(known_args.amount_filter)
    year = dt.datetime(year=int(known_args.year_filter), month=1, day=1, tzinfo=pytz.UTC)

    with pipeline as p:
        df = p | 'Read CSV' >> read_csv(known_args.input, parse_dates=['timestamp'])
        results = run_transform(df, amount, year)
        # TODO: Figure out why .to_csv() produces blank lines, the official Beam taxiride example has the same issue:
        # https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/dataframe#running-location_id_agg
        results.to_csv('output/results.csv')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
