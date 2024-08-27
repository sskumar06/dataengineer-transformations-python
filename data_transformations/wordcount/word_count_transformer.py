import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,explode,count

def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    logging.info("Reading text file from: %s", input_path)
    input_df = spark.read.text(input_path)

    logging.info("Writing csv to directory: %s", output_path)
    dfwords = input_df.withColumn('words', split(col('value'), ' ')) \
        .withColumn('word', explode(col('words'))) \
        .drop('value', 'words').groupBy('word').agg(count('word') \
                                                    .alias('count')).orderBy('count', ascending=False)

    dfwords.coalesce(1).write.csv(output_path, header=True)
