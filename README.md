[![Build Status](https://travis-ci.org/maropu/spark-tpcds-datagen.svg?branch=master)](https://travis-ci.org/maropu/spark-tpcds-datagen)

This is the TPCDS data generator for Apache Spark, which is split off from [spark-sql-perf](https://github.com/databricks/spark-sql-perf)
and includes pre-built [tpcds-kit](https://github.com/davies/tpcds-kit) for Mac/Linux x86_64 platforms.
<!--
To check performance regression for TPCDS, the benchmark results (sf=5) and codegen metrics of the current Spark master
are daily tracked in the Google Spreadsheet ([performance charts](https://docs.google.com/spreadsheets/d/1V8xoKR9ElU-rOXMH84gb5BbLEw0XAPTJY8c8aZeIqus/edit?usp=sharing) and [metric charts](https://docs.google.com/spreadsheets/d/1MP4q9pVpXWt-cL75brdyQmaZ6uhLhTnLg2ZXv2VLnoQ/edit?usp=sharing)).
Also, the validation results of the TPCDS queries are stored in [reports/tests](./reports/tests).
 -->

## How to generate TPCDS data

First of all, you need to set up Spark:

    $ git clone https://github.com/apache/spark.git

    $ cd spark && ./build/mvn clean package -DskipTests

    $ export SPARK_HOME=`pwd`

Then, you can generate TPCDS test data in `/tmp/spark-tpcds-data`:

    $ ./bin/dsdgen --output-location /tmp/spark-tpcds-data

## How to run TPC-DS queries in Spark

You can run TPC-DS quries by using test data in `/tmp/spark-tpcds-data`:

    $ ./bin/spark-submit \
        --class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark \
        sql/core/target/spark-sql_<scala.version>-<spark.version>-tests.jar \
        --data-location /tmp/spark-tpcds-data

## Options for the generator

    $ ./bin/dsdgen --help
    Usage: spark-submit --class <this class> --conf key=value <spark tpcds datagen jar> [Options]
    Options:
      --output-location [STR]                Path to an output location
      --scale-factor [NUM]                   Scale factor (default: 1)
      --format [STR]                         Output format (default: parquet)
      --overwrite                            Whether it overwrites existing data (default: false)
      --partition-tables                     Whether it partitions output data (default: false)
      --use-double-for-decimal               Whether it prefers double types (default: false)
      --cluster-by-partition-columns         Whether it cluster output data by partition columns (default: false)
      --filter-out-null-partition-values     Whether it filters out NULL partitions (default: false)
      --table-filter [STR]                   Queries to filter, e.g., catalog_sales,store_sales
      --num-partitions [NUM]                 # of partitions (default: 100)

## Run specific TPC-DS quries only

To run a part of TPC-DS queries, you type:

    $ ./bin/run-tpcds-benchmark --data-location [TPC-DS test data] --query-filter "q2,q5"

## Other helper scripts for benchmarks

To quickly generate the TPC-DS test data and run the queries, you just type:

    $ ./bin/report-tpcds-benchmark [output file]

This script finally formats performance results and appends them into ./reports/tpcds-avg-results.csv.
Notice that, if SPARK_HOME defined, the script uses the Spark.
Otherwise, it automatically clones the latest master in the repository and uses it.
To check performance differences with pull requests, you could set a pull request ID in the repository as an option
and run the quries against it.

    $ ./bin/report-tpcds-benchmark [output file] [pull request ID (e.g., 12942)]

## Bug reports

If you hit some bugs and requests, please leave some comments on [Issues](https://github.com/maropu/spark-sql-server/issues)
or Twitter([@maropu](http://twitter.com/#!/maropu)).

