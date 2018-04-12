# data-compare

This is a tool to compare data in Hyena and Presto. It is intended to be a part of the Hyena 
development/testing pipeline.

## Usage

FIXME: explanation

    $ java -jar target/uberjar/data-compare-0.1.0-SNAPSHOT-standalone.jar [args]
    where args are:
    -h, --help           Print this help
    -m, --min TIMESTAMP  Lower bound for timestamps
    -x, --max TIMESTAMP  Upper bound for timestamps
    -f, --file FILENAME  Name of the file describing data columns


