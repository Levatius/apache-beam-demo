# Apache Beam Demo

In this project we will be exploring how to use Apache Beam to process .csv files.

The traditional approach involves using Beam primitives to process elements directly.
However, with the introduction of Beam's 
[Dataframe API](https://beam.apache.org/documentation/dsls/dataframes/overview/), we now can use the arguably more 
accessible pandas-like syntax to process DeferredDataFrame objects instead.

### Set Up
To install the packages required for this project, execute:
```shell
pip install -r requirements.txt
```

### Parameters
- `--input`: Input CSV to process. Defaults to `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
- `--amount_filter`: Filter transactions to those strictly greater than this amount. Defaults to `20`.
- `--year_filter`: Filter transactions to those made during or after this year. Defaults to `2010`.

## Traditional Pipeline
Uses Beam primitives to process the CSV with a single Composite Transform.

To run locally with the default parameters, execute:
```shell
python -m traditional_pipeline
```

Ensure `output/` is empty before re-running the pipeline.

## Dataframe Pipeline
Uses Beam's Dataframe API to process the CSV with pandas-like syntax.

To run locally with the default parameters, execute:
```shell
python -m dataframe_pipeline
```

Ensure `output/` is empty before re-running the pipeline.