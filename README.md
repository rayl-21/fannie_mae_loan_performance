# Fannie Mae Loan Performance Data

## About the dataset
[Fannie Mae single-family loan performance data](https://capitalmarkets.fanniemae.com/credit-risk-transfer/single-family-credit-risk-transfer/fannie-mae-single-family-loan-performance-data) is a dataset provided by Fannie Mae to help better understand the credit performance of Fannie Mae mortgage loans. The raw dataset is ~40GB in the zip format and organized by acquisition quarters (e.g. 2000Q1.csv, 2000Q2.csv, etc.). Fully expanded data takes ~524GB (>2 billion rows) in PostgreSQL database. Personal computers/laptops cannot process such a large dataset, but it will be a good data engineering exercise to set up a local database and ingest data into it.

The dataset itself does not contain any column information, but this [R script](https://capitalmarkets.fanniemae.com/media/document/zip/FNMA_SF_Loan_Performance_r_Primary.zip) provides names and types for each column. Column names and types have been extracted and saved in [lp_columns.csv](/lp_columns.csv).

## ETL exercise
High-level walkthrough of the ingestion logic:
- Set up a local PostgreSQL database to store the dataset. Using default configurations of PostgreSQL is enough for this exercise. PostgreSQL can be [downloaded here](https://www.postgresql.org/download/).
- Create a table in the database.
- Uncompress and ingest data files saved in csv format one at a time, since sizes of csv files vary from 1GB to >25GB and uncompressing all files at once might eat up the whole disk space.
- Delete the data file.

The ingestion time takes ~7.2 hours to complete.

NOTE: This is a stand-alone exercise and the result will not be used in downstream tasks since it is beyond a laptop's capability to process the data even in the database environment. For context, a sinlge `COUNT(*)` command takes forever to run (recall that this is a table with oevr 2 billion rows).

## Data wrangling
Due to the very large size of the dataset, we need to process the data sequentially. That is, calculate statistics for each data file and combine them in the end. However, sequential processing still poses a out-of-memory challenge to the devie on which data is being analyzed. One solution is to use packages designed for tackling out-of-memory issues, such as [datatable](https://github.com/h2oai/datatable) and [dask](https://github.com/dask/dask).

(Under construction...)

## Analysis
(Under construction...)
