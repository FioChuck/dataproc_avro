# Avro Ingestion into Bigquery

Objective: Load large block Avro files into GCP Bigquery using Pyspark running on GCP Dataproc.

# Ingestion using Command-line Tooling

Avro files can typically be loaded into Bigquery using the `bq load` command-line tool with the `--source_format` flag set to _AVRO_. More information on this operation can be found here: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#bq_1

See example below:

```shell
bq load \
    --source_format=AVRO \
    mydataset.mytable \
    gs://mybucket/mydata.avro
```

Unfortunately a few limitations apply when using this toolset. These limitations are tabulated below. More info here: https://cloud.google.com/bigquery/quotas

1. (`bq` command-line tool) Maximum size for file data blocks: 16 MB
   > The size limit for Avro file data blocks is 16 MB.
2. (`jobs.query` API) Maximum row size: 100 MB
   > The maximum row size is approximate, because the limit is based on the internal representation of row data. The maximum row size limit is enforced during certain stages of query job execution.

This means you cannot use the `bq load` command-line tool to load Avro data with a block size greater than 16 MB. The data must be manipulated prior to being loaded.

# Ingestion using Spark

Test files ingested into Spark dataframe using spark-avro dependency located here: https://mvnrepository.com/artifact/org.apache.spark/spark-avro

spark-avro moducle is external, there is no .avro API in DataFrameReader or DataFrameWriter. The dependency must be added to cluster or loaded as package during spark-submit. For more see: https://spark.apache.org/docs/latest/sql-data-sources-avro.html

Example:

```python
df = spark.read.format("avro").load("examples/src/main/resources/users.avro")
df.select("name", "favorite_color").write.format("avro").save("namesAndFavColors.avro")
```

---

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
