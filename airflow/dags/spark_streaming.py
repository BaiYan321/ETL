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
   spark = SparkSession.builder.appName("Kafka-Spark-Streaming").getOrCreate()
   try:
   # Read from Kafka topic
      df = spark.readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka1:29092") \
      .option("subscribe", "etl_topic") \
      .load()
      # Add more fields based on the actual structure of your JSON
      logging.info("Initial dataframe created successfully")
      query = df.show()
      #query.awaitTermination()
   except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")
   return query.awaitTermination()

if __name__ == "__main__":
    
   # file_path = "/data/marketstack.json"
   # process(file_path)
   import_kafka_nyt_data()