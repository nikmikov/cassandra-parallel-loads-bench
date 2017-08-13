# Testing Apache Cassandra inserts performance

## Setup

Install Cassandra Cluster Management following instructons [here](https://github.com/pcmanus/ccm)

Create cluster `test` with 3 nodes:

`ccm create test -v 3.11 -n 3`

Start cluster

`ccm start`

Create 1GB binary file:

`dd if=/dev/urandom of=$TMPDIR/data.bin bs=1m count=1000`

## Run

You can run it via sbt runner or building and running fat jar directly

### Run with SBT runner

`sbt run --input $TMPDIR/data.bin`

or

`sbt -Dorg.slf4j.simpleLogger.defaultLogLevel=debug "run --input $TMPDIR/data.bin"`


to enable debug output


### Run from fat jar

`sbt assembly` will build a fat jar


Run it with

`java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -cp target/scala-2.12/*-assembly*.jar com.mikoff.pg.CassandraLoad --input $TMPDIR/data.bin --batch 1 --parallel 4 --block-size 10 --async-max 512`


## Command line parameters

`--host <hostname>` and `--port <port>`: Cassandra cluster contact point

`--batch <N>`: Batch insert statement into N UNLOGGED batches. If N <= 1, then disable batching. Default: 1

`--parallel <N>`: Split input file into N chunks and insert chunks in parallel. Default: 1

`--block-size <N>`: Size of message block in input file in bytes. Default: 10

`--async-max <N>`: Maximum number of simultaneous executeAsync from each worker thread. Default: 124

`--input <filename>`: input file path
