# Avro Ingestion into BigQuery

Objective: Load large block Avro files into GCP BigQuery.

Avro is a preferred format for loading data into BigQuery since the data is read in parallel. More info here: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#advantages_of_avro

This project was created to highlight two options for loading Avro files into BigQuery. These options are documented below. The Pyspark code found in the /src folder can be used as a starting point for loading Avro files, applying transformations, and saving to BigQuery. Continue reading below for more info on this process.

# Ingestion using Command-line Tooling

Avro files can typically be loaded into BigQuery using the `bq load` command-line tool with the `--source_format` flag set to _AVRO_. More information on this operation can be found here: https://cloud.google.com/BigQuery/docs/loading-data-cloud-storage-avro#bq_1

See example below:

```shell
bq load \
    --source_format=AVRO \
    mydataset.mytable \
    gs://mybucket/mydata.avro
```

Unfortunately a few limitations exist when using this toolset. These limitations are tabulated below. More info here: https://cloud.google.com/bigquery/quotas

1. (`bq` command-line tool) Maximum size for file Avro data blocks: 16 MB
   > The size limit for a single Avro data blocks is 16 MB.
2. (`jobs.query` API) Maximum row size: 100 MB
   > The maximum row size is approximate, because the limit is based on the internal representation of row data. The maximum row size limit is enforced during certain stages of query job execution.

This means the `bq load` command-line tool cannot be used to load Avro data with a block size greater than 16 MB. The data must be manipulated prior to load. Secondly, once data is loaded into BigQuery, a query cannot return rows greater than 100 MB. If a row exceeds 100 MB, the entire row cannot be queried at one time - instead partial columns or STRUCT elements must by selected/filtered.

In order to bypass these limitations, Spark and Dataproc was explored as an alternative.

# Ingestion using Spark

The Spark Engine and BigQuery connector were also explored as an alternative for transforming and loading large Avro files into BigQuery. The following section descibes steps followed to test the BigQuery connector maintained by Google. The example code for this test is located in the following folder:

    ├── src                <- source code for use in this project
    │   └── ingestion      <- scripts to ingest and manipulate Avro files
    │       └── ingestion.py

Test files were ingested into a Spark dataframe using the _spark-avro_ dependency located here: https://mvnrepository.com/artifact/org.apache.spark/spark-avro. This dependency is requred to load AVRO files into the Spark engine.

The _spark-avro_ module is external - unfortunately the DataFrameReader and DataFrameWriter doesn't have native AVRO support. The dependency must be added to the cluster or loaded as package during job submission _(spark-submit)_. For more see: https://spark.apache.org/docs/latest/sql-data-sources-avro.html

Read AVRO files from GCS Example:

```python
df = spark.read.format("avro").load("examples/src/main/resources/users.avro")
df.select("name", "favorite_color").write.format("avro").save("namesAndFavColors.avro")
```

Next the test data _(Spark dataframes loaded using the code above)_ was loaded into BigQuery using the Dataproc connector found here: https://github.com/GoogleCloudDataproc/spark-BigQuery-connector. This connector is maintained by Google.

Notice the _explode_ portion of ingestion.py. This functionality is usefull for un-nesting dataframes. The example data used in this exploration had deeply nested fields causing single blocks/rows to exceed 100 MB in BigQuery. As mentioned previously, rows over 100 MB cannot be queried in a single statement. The un-nesting functionality was used to reduce row size.

The following CLI can be used to create a Spark Cluster and deploy the code in this project.

1. Create a Dataproc cluster with dependencies _(Avro and BQ)_:

Notice packages are added for connecting to BigQuery and ingesting Avro files into Spark. Also, the Dataproc setting publishes Dataproc job driver output and Spark driver logs to logging.

```bash
gcloud dataproc clusters create <cluster name> \
--enable-component-gateway \
--region <region> \
--zone <zone> \
--master-machine-type n1-standard-4 \
--master-boot-disk-type pd-ssd \
--master-boot-disk-size 500 \
--num-workers 2 \
--worker-machine-type n1-standard-4 \
--worker-boot-disk-type pd-ssd \
--worker-boot-disk-size 500 \
--image-version 2.0-debian10 \
--scopes 'https://www.googleapis.com/auth/cloud-platform' \
--metric-sources spark,yarn \
--properties=^~^dataproc:dataproc.logging.stackdriver.enable=true~dataproc:dataproc.logging.stackdriver.job.yarn.container.enable=true~spark:spark.jars.packages=org.apache.spark:spark-avro_2.12:3.1.3,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2
```

2. Submit Spark job

```bash
gcloud dataproc jobs submit pyspark \
<.py gcs location> \
--region=<region> \
--cluster=<cluster name>-- --file_loc=<avro file gcs location> --dataset=< bq dataset name> --table=<bq table name> --temp_bucket=<gcs temp bucket> --temp_format=avro --explode_col=none
```

## Known Error when writing Spark Dataframe arrays of structs to BigQuery.

Data written to BigQuery using the Dataproc connector is first written to Google Cloud Storage as an intermediate file. By default this intermediate file format is Parquet. Unfortunately when writing an array of structs with default settings the schema is corrupted. This is a know issue documented here: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/251

To work around this the `intermediateFormat` flag can be set to _avro_. Unfortunately intermediate Avro files are still subject to the max block size of 16 MB when loaded to BigQuery. This limitation applies to the `jobs.insert` API method used by the connector. This means Spark and the Dataproc connector are subject to the same limitations as the `bq load` command-line tool.

For example:

```python
df.write \
.format("bigquery") \
.option("table", "{}.{}".format(<dataset>, <table>)) \
.option("temporaryGcsBucket", <bucket>) \
.option("intermediateFormat", "avro") \
.mode('overwrite') \
.save()
```

> **UPDATE 11/09/2022**

> Recently Google added an `enableListInference` option to the Dataproc BigQuery connector. This option removes redundant structure introduced by the standard Parquet format when writing an array of structs. This should resolve any issues described above. When writing AVRO files with the default _intermediateFormat_ and _enableListInference_, large block files can be loaded without issue. Keep in mind, Querying the data will still be constrained to 100 MB.

Updated Example:

```python
df.write \
.format("bigquery") \
.option("table", "{}.{}".format(<dataset>, <table>)) \
.option("temporaryGcsBucket", <bucket>) \
.option("enableListInference", "true") \
.mode('overwrite') \
.save()
```

More info on List Logical Types here: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#list_logical_type

_Direct Mode_ is another option for loading Avro files with an array of structs. The Direct Mode option uses the BigQuery Storage Write API which is not subject to the same quotas as the BiqQuery API.

Unforunatley the Direct Mode option does not support MapType data types. See issue here: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/522

The example below shows how to enable Direct Mode.

```python
df.write \
.format("bigquery") \
.option("table", "{}.{}".format(<dataset>, <table>)) \
.option("temporaryGcsBucket", <bucket>) \
.option("writeMethod", "direct") \
.mode('overwrite') \
.save()
```

## Checking Avro Block Sizes

This project contains a Python script for analyzing Avro block size. The results can be useful when deciding how to load data into BigQuery. For example, blocks over 100 MB likely cannot be queried in a single statement.

The example code for this test is located in the following folder:

    ├── src                <- source code for use in this project
    │   └── stats          <- scripts to analyze Avro block size
    │       └── avro_stats.py

The script requires an Avro file path argument and returns the following stats:

- total bytes
- total rows
- total blocks
- average row size
- average block size
- largest block size

---

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
