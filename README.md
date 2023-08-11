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

The function "R/generate_indicators" generates the new indicators published on swissjobtracker
To compute the index on your local computer for test purposes use the file inst/compute_index_example/compute_indices.R which will initiate the DB connections (if you have access to the DB) and call the generate indicator function

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

The database structure can be recreated with
[migrate](https://github.com/golang-migrate/migrate) or by running the scripts
in `inst/db/migrations`.

To add a new migration:

```bash
migrate create -dir inst/db/migrations -ext sql -seq "<name>"
```

Then edit the newly created files (both `up.sql` and `down.sql`).

To test the migrations, the easiest way is to use docker:

1. Start the postgres docker container:

```bash
docker run --rm -it --name migration_test -e POSTGRES_PASSWORD="<password>" -e POSTGRES_USER=kofadmin -e POSTGRES_DB=nrp77 -p 5432:5432 postgres
```

2. [Optional] Create the schema where the tables are to be created

```bash
docker exec -it migration_test psql -U kofadmin -W nrp77 -c "CREATE SCHEMA x28"
```

3. Run the migration

```bash
# If the schema was not created, remove "search_path=x28"
migrate -database "postgresql://kofadmin:<password>@localhost/nrp77?search_path=x28&sslmode=disable" -path inst/db/migrations up
```

4. Test also rolling back the migration

```bash
# If the schema was not created, remove "search_path=x28"
migrate -database "postgresql://kofadmin:<password>@localhost/nrp77?search_path=x28&sslmode=disable" -path inst/db/migrations down 1
```

At any point you can inspect the database state by connecting with `psql` and
running SQL queries:

```bash
docker exec -it migration_test psql -U kofadmin -W nrp77
```

Once the migration is ready to be applied, someone with the correct access to
the database must run the migration on the production system.
