from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, explode_outer
import time

def flatten(df):
   #compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df

def start_streaming_app(max_retries=3, retry_delay=10):
    retries = 0
    spark = None
    while retries < max_retries:
        try:
            spark = SparkSession.builder \
                .appName("Kafka-Spark-Streaming") \
                .config("spark.driver.extraClassPath", "/data/clickhouse-jdbc.jar") \
                .getOrCreate()

            # Read from Kafka topic
            df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka1:29092") \
                .option("subscribe", "etl_topic") \
                .load()

            # Transform the data as needed, example: cast the value as a string
            # df = df.selectExpr("CAST(value AS STRING)")
            #########################################################
            df=df.drop('pagination')

            df = flatten(df)

            df = df.select(col("data_symbol").alias("Symbol"), 
                               col("data_date").alias("Date"), 
                               col("data_open").alias("Open"), 
                               col("data_close").alias("Close"), 
                               col("data_low").alias("Low"), 
                               col("data_high").alias("High"),  
                               col("data_volume").alias("Volume"))
      
            df = df.withColumn('Date', df['Date'].cast('date')).orderBy("Date")

            df = df.withColumn('Growth', (df['Close'] - df['Open']))\
                    .withColumn('Growth%', (df['Close'] - df['Open'])/df['Close'])
            #########################################################

            # Write the output to the console (for testing, use appropriate sink in production)
            clickhouse_url = "jdbc:clickhouse://clickhouse:8123/marketstack_db"
            
            query = df.write \
                .format("jdbc") \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("url",clickhouse_url) \
                .option("dbtable", "marketstack_db.marketstack_streaming") \
                .option("user", "default") \
                .option("password", "default") \
                .mode("append") \
                .save()
                
            query = query.writeStream \
                        .outputMode('append') \
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
