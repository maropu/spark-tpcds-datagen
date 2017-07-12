This is the TPCDS data generator for Apache Spark, which is split off from [spark-sql-perf](https://github.com/databricks/spark-sql-perf)
and includes pre-built [tpcds-kit](https://github.com/davies/tpcds-kit) for x86_64 on Mac/Linux platforms.

## How to generate TPCDS data

First of all, you need to set your Spark location at `SPARK_HOME`:

    $ export SPARK_HOME=<Your Spark>

Then, you can generate TPCDS test data in `/tmp`:

    $ ./bin/dsdgen /tmp

## Options for the generator

    $ ./bin/dsdgen --help

    Usage: ./bin/dsdgen [options] [output dir]
    ...
    dsdgen options:
      --conf spark.sql.dsdgen.scaleFactor=NUM                    Scale factor (Default: 1).
      --conf spark.sql.dsdgen.format=STR                         Output format (Default: parquet).
      --conf spark.sql.dsdgen.overwrite=BOOL                     Wheter it overwrites existing data (Default: false).
      --conf spark.sql.dsdgen.partitionTables=BOOL               Wheter it partitions output data (Default: false).
      --conf spark.sql.dsdgen.useDoubleForDecimal=BOOL           Wheter it prefers double types (Default: false).
      --conf spark.sql.dsdgen.clusterByPartitionColumns=BOOL     Wheter it cluster output data by partition columns (Default: false).
      --conf spark.sql.dsdgen.filterOutNullPartitionValues=BOOL  Wheter it filters out NULL partitions (Default: false).
      --conf spark.sql.dsdgen.tableFilter=STR                    Filters a specific table.
      --conf spark.sql.dsdgen.numPartitions=NUM                  # of partitions (Default: 100).

## Bug reports

If you hit some bugs and requests, please leave some comments on [Issues](https://github.com/maropu/spark-sql-server/issues)
or Twitter([@maropu](http://twitter.com/#!/maropu)).

