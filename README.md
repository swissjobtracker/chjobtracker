# nrp77-computation

Computation Backend for the Labor Market Dashboard developed in NRP 77

## Calculating the indices

The main entrypoint for calculating the final series is `generate_indicators`.

It performs the following steps:

1.  Load all ad data from the database

2.  Remove ads from Liechtenstein

3.   Perform data cleaning (see `prepare_ads`)

4.  Filter portals with unusual activity to be disregarded in certain weeks

5.   Merge additional data (e.g. canton) where necessary and calculate the indices (see `make_series`)

6.   Return a list of time series objects (usually to be stored in the database right away)

## Reading in data updates

New advertisement data is read in regularly through the function `x28_update_db`

## Reading in bulk deliveries

The workflow for reading in bulk deliveries from x28 goes:

-   transform `ndjson` to R data objects (one per year) with `dumps_to_rds`
-   join the different tables across years into `csv` with `rds_to_csv`
-   read the `csv` into the database with `csv_to_db`
-   amend deletion dates missed across newyear with `inst/data_deliveries/amend_deletion_dates.R`

All this should not have to be done anymore as new data is read in live.

# Database structure

A diagram of the planned tables can be found at <https://dbdiagram.io/d/6025332880d742080a3a2613>
