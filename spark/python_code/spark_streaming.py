from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType, StringType, StructField
from pyspark.sql.functions import col, explode_outer, row_number, avg ,when
from pyspark.sql import Window
import sys
import os

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

def import_kafka_nyt_data():
   spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .getOrCreate()
   try:
    # Read streaming data from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:29092") \
        .option("subscribe", "etl_topic") \
        .load()

    # Processing the Kafka data
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream \
        .format("console") \
        .start() \
        .awaitTermination()
   except Exception as e:
        logging.error(f"Initial dataframe couldn't be created due to exception: {e}")
if __name__ == "__main__":
   import_kafka_nyt_data()