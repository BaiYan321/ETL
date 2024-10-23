from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time


def start_streaming_app(max_retries=3, retry_delay=10):
    retries = 0
    spark = None
    while retries < max_retries:
        try:
            spark = SparkSession.builder \
                .appName("Kafka-Spark-Streaming") \
                .getOrCreate()

            # Read from Kafka topic
            df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka1:29092") \
                .option("subscribe", "etl_topic") \
                .load()

            # Transform the data as needed, example: cast the value as a string
            df = df.selectExpr("CAST(value AS STRING)")

            # Write the output to the console (for testing, use appropriate sink in production)
            query = df.writeStream \
                .outputMode("append") \
                .format("console") \
                .start()

            # Await termination of the streaming query
            query.awaitTermination()
            print(
                f"Streaming job started successfully on attempt {retries + 1}.")
            break  # Exit the loop if successful

        except Exception as e:
            retries += 1
            print(f"Error occurred: {e}. Attempt {retries} of {max_retries}.")
            if retries >= max_retries:
                print("Max retries reached. Shutting down the job.")
                if spark:
                    spark.stop()
            else:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)  # Wait before retrying


# Start the streaming app with retries
start_streaming_app(max_retries=3, retry_delay=10)
