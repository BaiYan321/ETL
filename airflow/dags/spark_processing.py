from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, explode_outer, row_number, avg ,when
from pyspark.sql import Window
import sys
import os

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

def process_data(file_path):
    
    spark = SparkSession.builder.appName("DataProcessing").getOrCreate()
    
    if os.path.exists(file_path):

      df = spark.read.format("json") \
               .option("multiLine", True) \
               .option("header",True) \
               .option("inferschema",True) \
               .load(file_path)

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
      
      # Define a window specification
      windowSpec20 = Window.orderBy("Symbol").rowsBetween(-19, 0)
      windowSpec60 = Window.orderBy("Symbol").rowsBetween(-59, 0)

      # Calculate the 20-day/60-day moving average
      df = df.withColumn("20DayMA", avg(col("Close")).over(windowSpec20))
      df = df.withColumn("60DayMA", avg(col("Close")).over(windowSpec60))

      # Define a window specification
      window_spec = Window.orderBy("Date")  # Replace "some_column" with a column you want to order by

      # Create an index column
      df = df.withColumn("index", row_number().over(window_spec))

      # Apply conditional logic based on the index
      df = df.withColumn("20-DayMA", when(col("index") < 20, None).otherwise(col("20DayMA")))
      df = df.withColumn("60-DayMA", when(col("index") < 60, None).otherwise(col("60DayMA")))
      df = df.drop("index","20DayMA","60DayMA")

      df = df.withColumn("Flactuation", ((df['High'] - df['Low'])/df['Low']))

      df.printSchema()
      df.show()
    else:
      print("File not found!")

    return df

if __name__ == "__main__":
    
   file_path = "/data/marketstack.json" # ./nyt.json不行 nyt.json不行 /nyt.json不行 ./dags/nyt.json 不行 ./airflow/nyt.json 不行 ../nyt.json 不行 /airflow/nyt.json不行 ../nyt.json不行
   # file_path = "../nyt.json"
   process_data(file_path)
