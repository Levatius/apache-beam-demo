# Apache Beam Demo

In this project we will be exploring how to use Apache Beam to process .csv files.

The traditional approach involves using Beam primitives to process elements directly.
However, with the introduction of Beam's 
[Dataframe API](https://beam.apache.org/documentation/dsls/dataframes/overview/), we now can use the arguably more 
accessible `pandas` syntax to process DeferredDataFrame objects instead.

## Set Up
To install the packages required for this project, execute:
```shell
pip install -r requirements.txt
```

## Traditional Pipeline
I have not yet implemented this, but will do so for comparison purposes.

## Dataframe Pipeline

### Data
Sample transactions data is available to us here:
- `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`

The pipeline can accept a custom .csv file via the `--input` parameter so long as it contains the same columns.

### Run
The pipeline has two more parameters to demonstrate filtering:
- `--amount_filter`: Filter transactions to those strictly greater than this amount. Defaults to 20.
- `--year_filter`: Filter transactions to those made during or after this year. Defaults to 2010.

To run locally with default parameters, execute:
```shell
python -m dataframe_pipeline
```