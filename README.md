# Avro Ingestion into BigQuery

Objective: Load large block Avro files into GCP BigQuery using Pyspark running on GCP Dataproc.

Avro is a preferred format for loading data into BigQuery since the data is read in parallel. More info here: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#advantages_of_avro

This project was created to highlight two options for loading Avro files into BigQuery. These options are documented below. The Pyspark code in the src folder can be used as a starting point for loading Avro files, applying transformations, and saving to BigQuery. Continue reading below for more info on this process.

# Ingestion using Command-line Tooling

Avro files can typically be loaded into BigQuery using the `bq load` command-line tool with the `--source_format` flag set to _AVRO_. More information on this operation can be found here: https://cloud.google.com/BigQuery/docs/loading-data-cloud-storage-avro#bq_1

See example below:

```shell
bq load \
    --source_format=AVRO \
    mydataset.mytable \
    gs://mybucket/mydata.avro
```

Unfortunately a few limitations apply when using this toolset. These limitations are tabulated below. More info here: https://cloud.google.com/BigQuery/quotas

1. (`bq` command-line tool) Maximum size for file data blocks: 16 MB
   > The size limit for Avro file data blocks is 16 MB.
2. (`jobs.query` API) Maximum row size: 100 MB
   > The maximum row size is approximate, because the limit is based on the internal representation of row data. The maximum row size limit is enforced during certain stages of query job execution.

This means the `bq load` command-line tool cannot be used to load Avro data with a block size greater than 16 MB. The data must be manipulated prior to load. Secondly, once data is loaded into BigQuery, a query cannot return rows greater than 100 MB. If a row exceeds 100 MB, the entire row cannot be queried at one time.

In order to bypass these limitations, Spark and Dataproc was explored as an alternative.

# Ingestion using Spark

The Spark Engine and BigQuery connector were also explored as an alternative for transforming and loading large Avro files. The example code for this project is located in this folder:

    ├── src                <- Source code for use in this project.
    │   └── ingestion      <- Scripts to ingest and manipulate Avro files
    │       └── ingestion.py

Test files were ingested into a Spark dataframe using _spark-avro_ dependency located here: https://mvnrepository.com/artifact/org.apache.spark/spark-avro

The _spark-avro_ moducle is external, there is no .avro API in DataFrameReader or DataFrameWriter. The dependency must be added to cluster or loaded as package during spark-submit. For more see: https://spark.apache.org/docs/latest/sql-data-sources-avro.html

Example:

```python
df = spark.read.format("avro").load("examples/src/main/resources/users.avro")
df.select("name", "favorite_color").write.format("avro").save("namesAndFavColors.avro")
```

Next the test data was loaded into BigQuery using the Dataproc connector found here: https://github.com/GoogleCloudDataproc/spark-BigQuery-connector

I was able to sucessfully deploy the Spark code in this project and load Avro files in GCS to BigQuery. Notice the _explode_ portion of ingestion.py. This functionality is usefull for un-nesting dataframes. The example data used in this exploration had nested fields causing single rows to exceed 100 MB in BigQuery. As mentioned previously, these rows cannot be queried. The un-nesting functionality was used to reduce row size.

The following CLI can be used to create a Spark Cluster and deploy the code in this project.

1. Create a Dataproc cluster with dependencies _(Avro and BQ)_:

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

Data written to Biquery is first written to Google Cloud Service as an intermediate file. By defaul this intermediate file format is Parquet. Unfortunately when writing an array of structs with default settings the schema is corrupted. This is a know issue documented here: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/251

To work around this the `intermediateFormat` flag should be set to _avro_.

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

---

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
