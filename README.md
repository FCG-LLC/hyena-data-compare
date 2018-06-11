# data-compare

This is a tool to compare data in Hyena and Presto. It is intended to be a part of the Hyena 
development/testing pipeline.

## Building

    $ lein uberjar

## Usage

    $ java -jar target/uberjar/data-compare-0.1.0-SNAPSHOT-standalone.jar [args]
    where args are:
    -h, --help           Print this help
    -m, --min TIMESTAMP  Lower bound for timestamps
    -x, --max TIMESTAMP  Upper bound for timestamps
    -f, --file FILENAME  Name of the file describing data columns

## More details

The input for the tool is:

* timestamps range
* a file with three columns. 

The columns are:

* Hyena column name (one or many)
* Kudu's table_name.column_name (one or many)
* [Optional] metainformation informing the tool about conversion between the columns

For example, IP numbers in Kudu are stored as two i64 columns, and in Hyena either 2 u64 columns or 1 u128 column. Tool needs to be told to convert one format to another, or both to a common format. The specification of the metainformation: TBD.

Basing on the column names, the tool will generate a SQL queries for Drill and Presto, then run them minding the provided timestamps range (probably adding `LIMIT` and `OFFSET` for batch compares to limit the amount of data downloaded from the DB in one go).
