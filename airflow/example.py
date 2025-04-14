from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Create a sample dataframe
data = [("John", 28), ("Jane", 35), ("Mike", 23), ("Zoe", 27)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, schema=columns)

# Show the dataframe content
df.show()

# Stop the Spark session
spark.stop()
