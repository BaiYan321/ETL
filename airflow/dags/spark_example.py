from pyspark.sql import SparkSession

def import_data(input_path, output_path):
    # Initialize Spark session
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Read dataset
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df.write.csv(output_path, header=True)

    # Stop the Spark session
    spark.stop()

if __name__=="__main__":
    input_data_path = "/data/train.csv"
    output_data_path = "/output/train.csv"

    import_data(input_data_path, output_data_path)

