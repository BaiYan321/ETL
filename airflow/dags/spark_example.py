from pyspark.sql import SparkSession
'''
def import_data(input_path, output_path):
    # Initialize Spark session
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Read dataset
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df.show()

    df.write.csv(output_path, header=True)

    # Stop the Spark session
    spark.stop()

'''
def import_data(input_path):
    # Initialize Spark session
    try:
        spark = SparkSession.builder.appName("example").getOrCreate()
    # Read dataset
        df = spark.read.csv(input_path, header=True, inferSchema=True)

        df.show()
    except Exception as e:
        print(f"Error reading CSV: {e}")

    # Stop the Spark session
    spark.stop()


if __name__=="__main__":
    input_data_path = "airflow//input_data//train.csv" #./ 不行 /不行 ././不行 ../不行 尝试在dag文件夹建立input folder
    #output_data_path = "airflow//output_data//train.csv"

    import_data(input_data_path)#, output_data_path)

