
from pyspark.sql import SparkSession
import sys

def import_data(input_path):
    spark = SparkSession.builder.appName("DataExtraction").getOrCreate()
    df = spark.read.json(input_path)
    df.show()
    spark.stop()

if __name__ == "__main__":
    
    input_data_path = "/airflow/nyt.json" # ./nyt.json不行 nyt.json不行 /nyt.json不行 ./dags/nyt.json 不行 ./airflow/nyt.json 不行 ../nyt.json 不行 /airflow/nyt.json不行

    import_data(input_data_path)
