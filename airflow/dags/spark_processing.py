from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, explode_outer

import sys

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

    df = spark.read.format("json") \
            .option("multiLine", True) \
            .option("header",True) \
            .option("inferschema",True) \
            .load(file_path)
    df.show()
    df=df.drop('pagination')

    df = flatten(df)
    df.printSchema()
    df.show()

    return df


'''
spark = SparkSession.builder.appName("DataExtraction").getOrCreate()

#Define the schema for the JSON file

schema = StructType([
    StructField("copyright", StringType(), True),
    StructField("response", StringType(), True),
    StructField("status", StringType(), True),
    # Add more fields based on the actual structure of your JSON
])

#df = spark.read.schema(schema).option("mode", "PERMISSIVE").json('../nyt.json')
df = spark.read.format("json") \
          .option("multiLine", True) \
          .option("header",True) \
          .option("inferschema",True) \
          .load("../nyt.json")

df = df.drop("copyright", "status")


#make the first row name of the column
new_header = df.first()
df_without_header = df.filter(df[col] != new_header[col] for col in df.columns)

# Step 3: Rename columns using the first row values
df_with_new_columns = df_without_header.toDF(*new_header)


#
#df1 = df.withColumn("response111", explode("response")) \
#        .withColumn('docs', col("response111.docs"))

# df1=df.withColumn("response111", split(df.response, ","))
#df1=df.withColumn("response", explode("response"))

#df2=df1.select(col("response.docs").alias("docs"))

#df_flatten = flatten(df)
#df_with_new_columns.show()
df.show()
spark.stop()

'''

if __name__ == "__main__":
    
   file_path = "./data/marketstack.json" # ./nyt.json不行 nyt.json不行 /nyt.json不行 ./dags/nyt.json 不行 ./airflow/nyt.json 不行 ../nyt.json 不行 /airflow/nyt.json不行 ../nyt.json不行
   # file_path = "../nyt.json"
   process_data(file_path)
