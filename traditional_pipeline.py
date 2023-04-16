import argparse
import datetime as dt
import logging
import pytz
from dateutil.parser import parse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def split_and_parse(transaction_str):
    transaction = transaction_str.split(',')
    return parse(transaction[0]), transaction[1], transaction[2], float(transaction[3])


def filter_by_amount(transaction, amount):
    return transaction[3] > amount


def filter_by_year(transaction, year):
    return transaction[0] > year


def reduce_to_date_and_amount(transaction):
    return str(transaction[0].date()), transaction[3]


def format_to_str(transaction):
    return ','.join((str(item) for item in transaction))


@beam.ptransform_fn
def CompositeTransform(pcoll, amount, year):
    return (
        pcoll
        | 'Split and Parse' >> beam.Map(split_and_parse)
        | 'Filter by Amount' >> beam.Filter(filter_by_amount, amount)
        | 'Filter by Year' >> beam.Filter(filter_by_year, year)
        | 'Reduce to Date and Amount' >> beam.Map(reduce_to_date_and_amount)
        | 'Group by Date' >> beam.CombinePerKey(sum)
        | 'Format to String' >> beam.Map(format_to_str)
    )


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
        _ = (
            p
            | 'Read CSV' >> beam.io.ReadFromText(known_args.input, skip_header_lines=True)
            | 'Apply Transform' >> CompositeTransform(amount, year)
            | 'Write CSV' >> beam.io.WriteToText('output/results', file_name_suffix='.csv', header='date,total_amount')
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
