# Databricks notebook source
# MAGIC %md
# MAGIC # Imports streamed data from kinesis, cleans the data and puts into separate delta tables

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Disable format checks during the reading of Delta tables */
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# MAGIC %md
# MAGIC # Defining methods to retrieve streams and write to dataframes

# COMMAND ----------

# Returns kinesis stream as a dataframe
def get_stream(stream_name: str):
    df = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName', stream_name) \
    .option('initialPosition','earliest') \
    .option('region','us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load()
    return df  

# COMMAND ----------

# Deserializes data from stream and returns dataframe
def deserialize_stream(stream, schema):
    dataframe = stream \
    .selectExpr("CAST(data as STRING)") \
    .withColumn("data", from_json(col("data"), schema)) \
    .select(col("data.*"))
    return dataframe

# COMMAND ----------

# Add nulls to dataframe column
def change_to_None(df, column, entry_value):
    clean_df = df.withColumn(column, when(col(column).like(entry_value), None).otherwise(col(column)))
    return clean_df

# COMMAND ----------

# Writes dataframe to delta table
def write_df_to_table(dataframe, name: str):
    dataframe.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"/tmp/kinesis/129fb6ae3c55_{name}_table_checkpoints/") \
    .table(f"129fb6ae3c55_{name}_table")

# COMMAND ----------

# define schemas for each of the dataframes
pin_schema = StructType([
    StructField("index", IntegerType()),
    StructField("unique_id", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("poster_name", StringType()),
    StructField("follower_count", StringType()),
    StructField("tag_list", StringType()),
    StructField("is_image_or_video", StringType()),
    StructField("image_src", StringType()),
    StructField("downloaded", IntegerType()),
    StructField("save_location", StringType()),
    StructField("category", StringType())
])
geo_schema = StructType([
    StructField("ind", IntegerType()),
    StructField("timestamp", TimestampType()),
    StructField("latitude", FloatType()),
    StructField("longitude", FloatType()),
    StructField("country", StringType())
])
user_schema = StructType([
    StructField("ind", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("age", StringType()),
    StructField("date_joined", TimestampType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Fetching kinesis streams and turning to dataframes

# COMMAND ----------

pin_stream = get_stream('streaming-129fb6ae3c55-pin')
geo_stream = get_stream('streaming-129fb6ae3c55-geo')
user_stream = get_stream('streaming-129fb6ae3c55-user')

# COMMAND ----------

df_pin = deserialize_stream(pin_stream, pin_schema)
df_geo = deserialize_stream(geo_stream, geo_schema)
df_user = deserialize_stream(user_stream, user_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning df_pin

# COMMAND ----------

replace_with_None = {
    "description": "No description available%",
    "follower_count": "User Info Error",
    "image_src": "Image src error.",
    "poster_name": "User Info Error",
    "tag_list": "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e",
    "title": "No Title Data Available"
}

for key, value in replace_with_None.items():
    df_pin = change_to_None(df_pin, key, value)

# COMMAND ----------

df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))

df_pin = df_pin.withColumn("follower_count", col("follower_count").cast('int'))

# COMMAND ----------

df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

# COMMAND ----------

df_pin = df_pin.withColumnRenamed("index", "ind")

# COMMAND ----------

df_pin = df_pin.select(
    "ind",
    "unique_id",
    "title",
    "description",
    "follower_count",
    "poster_name",
    "tag_list",
    "is_image_or_video",
    "image_src",
    "save_location",
    "category"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning df_geo

# COMMAND ----------

df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))
df_geo = df_geo.drop("latitude", "longitude")

# COMMAND ----------

df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))

# COMMAND ----------

df_geo = df_geo.select(
    "ind",
    "country",
    "coordinates",
    "timestamp"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning df_user

# COMMAND ----------

df_user = df_user.withColumn("user_name", concat("first_name",lit(" "), "last_name"))
df_user = df_user.drop("first_name", "last_name")

# COMMAND ----------

df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))

# COMMAND ----------

df_user = df_user.select(
    "ind",
    "user_name",
    "age",
    "date_joined"
)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Display dataframes

# COMMAND ----------

df_pin.limit(10).display()
df_geo.limit(10).display()
df_user.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write data to delta tables

# COMMAND ----------

write_df_to_table(df_pin, "pin")
write_df_to_table(df_geo, "geo")
write_df_to_table(df_user, "user")
