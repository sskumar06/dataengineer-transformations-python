from pyspark.sql import SparkSession, DataFrame
import math
from pyspark.sql import functions as math_fun

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE


def compute_distance(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:

    dataframe = dataframe.withColumn("start_station_latitude_double", dataframe["start_station_latitude"].cast("double")) \
        .withColumn("end_station_latitude_double", dataframe["end_station_latitude"].cast("double")) \
        .withColumn("start_station_longitude_double", dataframe["start_station_longitude"].cast("double")) \
        .withColumn("end_station_longitude_double", dataframe["end_station_longitude"].cast("double"))

    dataframe = dataframe.withColumn('start_station_latitude_rad', dataframe['start_station_latitude_double'] * math.pi / 180)\
                        .withColumn('end_station_latitude_rad',dataframe['end_station_latitude_double'] * math.pi / 180)\
                        .withColumn('delta_latitude', (dataframe['end_station_latitude_double'] - dataframe['start_station_latitude_double']) * math.pi / 180)\
                        .withColumn('delta_longitude', (dataframe['end_station_longitude_double'] - dataframe['start_station_longitude_double']) * math.pi / 180)

    dataframe = dataframe.withColumn('a', math_fun.sin(dataframe['delta_latitude'] / 2) * math_fun.sin(dataframe['delta_latitude'] / 2) + \
    math_fun.cos(dataframe['start_station_latitude_rad']) * math_fun.cos(dataframe['end_station_latitude_rad']) * \
    math_fun.sin(dataframe['delta_longitude'] / 2) * math_fun.sin(dataframe['delta_longitude'] / 2))

    dataframe = dataframe.withColumn('c', 2 * math_fun.atan2(math_fun.sqrt(dataframe['a']), math_fun.sqrt(1 - dataframe['a'])))

    dataframe = dataframe.withColumn('d_in_meters', EARTH_RADIUS_IN_METERS * dataframe['c'])
    dataframe = dataframe.withColumn('d_in_miles', dataframe['d_in_meters'] / METERS_PER_MILE)
    return dataframe


def run(spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str) -> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(spark, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode='append')
