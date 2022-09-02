from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse


spark = SparkSession.builder.getOrCreate()

spark.conf.set(
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.avro.datetimeRebaseModeInRead", "CORRECTED")


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_loc', type=str)
    parser.add_argument('--dataset', type=str)
    parser.add_argument('--table', type=str)
    parser.add_argument('--temp_bucket', type=str)
    parser.add_argument('--temp_format', type=str)
    parser.add_argument('--explode_col', type=str)
    args = parser.parse_args()

    df = spark.read.format("avro").load(args.file_loc)

    df.repartition(64)

    if args.explode_col != "none":
        df = df.select("*", F.explode(F.col(args.explode_col))) \
            .drop(args.explode_col) \
            .withColumnRenamed('col', args.explode_col)

    print(df.count)
    df.printSchema()

    df.write \
      .format("bigquery") \
      .option("table", "{}.{}".format(args.dataset, args.table)) \
      .option("intermediateFormat", args.temp_format) \
      .option("temporaryGcsBucket", args.temp_bucket) \
      .mode('overwrite') \
      .save()

    # removed indermediateFormat to test Direct Mode
    #   .option("intermediateFormat", args.temp_format) \
    #   .option("temporaryGcsBucket", args.temp_bucket) \


if __name__ == '__main__':
    main()
