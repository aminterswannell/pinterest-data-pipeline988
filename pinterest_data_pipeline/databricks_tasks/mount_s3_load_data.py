# Databricks notebook source
# MAGIC %md 
# MAGIC # Mounting s3 bucket and loading the data

# COMMAND ----------

# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# COMMAND ----------

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

# AWS S3 bucket name
AWS_S3_BUCKET = "user-129fb6ae3c55-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/user-129fb6ae3c55-bucket"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# list the topics stored on the mounted S3 bucket
display(dbutils.fs.ls("s3n://user-129fb6ae3c55-bucket/topics"))

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Disable format checks during the reading of Delta tables */
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# File locations and types
pin_file_location = "s3n://user-129fb6ae3c55-bucket/topics/129fb6ae3c55.pin//partition=0/*.json"
geo_file_location = "s3n://user-129fb6ae3c55-bucket/topics/129fb6ae3c55.geo//partition=0/*.json"
user_file_location = "s3n://user-129fb6ae3c55-bucket/topics/129fb6ae3c55.user//partition=0/*.json"
file_type = "json"
# Ask Spark to infer the schemas
infer_schema = "true"
# Read in JSONs from mounted S3 buckets
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(pin_file_location)
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(geo_file_location)
df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(user_file_location)

# Display Spark dataframes
df_pin.limit(10).display()
df_geo.limit(10).display()
df_user.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning df_pin

# COMMAND ----------

def change_to_None(df, column, entry_value):
    clean_df = df.withColumn(column, when(col(column).like(entry_value), None).otherwise(col(column)))
    return clean_df

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
# MAGIC # Querying the data

# COMMAND ----------

from pyspark.sql.window import Window

df_pin_geo = df_pin.join(df_geo, df_pin.ind == df_geo.ind)
# create join of df_pin and df_user and a temporary view to run SQL query to create age group column
df_pin.join(df_user, df_pin.ind == df_user.ind).createOrReplaceTempView("category_age")
age_groups = spark.sql(
    "SELECT CASE \
        WHEN age between 18 and 24 then '18-24' \
        WHEN age between 25 and 35 then '25-35' \
        WHEN age between 36 and 50 then '36-50' \
        WHEN age > 50 then '50+' \
        END as age_group, * FROM category_age")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Find the most popular category in each country

# COMMAND ----------

# order by category_count column and partition by country column
window_popular_category = Window.partitionBy("country").orderBy(col("category_count").desc())
# use aggregation function with df_pin_geo dataframe and window function to find the most popular category in each country
df_pin_geo.groupBy("country", "category") \
.agg(count("category").alias("category_count")) \
.withColumn("ranking", row_number().over(window_popular_category)) \
.filter(col("ranking") == 1).drop("ranking") \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find which was the most popular category each year

# COMMAND ----------

# order by category_count column and partition by post_year column
window_popular_category_year = Window.partitionBy("post_year").orderBy(col("category_count").desc())
# use aggregation function with df_pin_geo dataframe and window function to find the most popular category in each year
df_pin_geo.withColumn("post_year", year("timestamp")) \
.filter(col("post_year") >= 2018).filter(col("post_year") <= 2022) \
.groupBy("post_year", "category").agg(count("category").alias("category_count")) \
.withColumn("ranking", row_number().over(window_popular_category_year)) \
.filter(col("ranking") == 1).drop("ranking") \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find the user with the most followers in each country

# COMMAND ----------

# order by follower_count column and partition by country column
window_followers_by_country = Window.partitionBy("country").orderBy(col("follower_count").desc())
# use df_pin_geo dataframe and window function to find the user with the most followers in each country
max_followers = df_pin_geo.withColumn("ranking", row_number().over(window_followers_by_country)) \
    .filter(col("ranking") == 1).select("country", "poster_name", "follower_count")

max_followers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find the country with the user with most followers

# COMMAND ----------

# find max followers from max_followers dataframe
max_user_followers = max_followers.select(max("follower_count")).collect()[0][0]
# use top result of max_followers dataframe to find the country with the user with the most followers
country_max_followers = max_followers.select("*").where(col("follower_count") == max_user_followers)
country_max_followers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find the most popular category for different age groups

# COMMAND ----------

# order by category_count column and partition by age_group column
window_popular_category_by_age = Window.partitionBy("age_group").orderBy(col("category_count").desc())
# from age_groups use aggregation function to find the most popular category by age group
age_groups.groupBy("age_group", "category").agg(count("category").alias("category_count")) \
.withColumn("rank", row_number().over(window_popular_category_by_age)) \
.filter(col("rank") == 1).drop("rank") \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find the median follower count for different age groups

# COMMAND ----------

# from age_groups use aggregation function to find the median followers ordered by age group
age_groups.select("user_name", "date_joined", "age_group", "follower_count") \
.distinct().groupBy("age_group").agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")) \
.orderBy("age_group") \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find how many users joined each year

# COMMAND ----------

# user user_df and aggregation function to find how many users joined in each year
df_user.withColumn("post_year", year("date_joined")).drop("ind").distinct() \
.filter(col("post_year") >= 2015).filter(col("post_year") <= 2020) \
.groupBy("post_year").agg(count("user_name").alias("number_users_joined")) \
.orderBy("post_year") \
.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Find the median follower count of users based on their joining year

# COMMAND ----------

# use age_groups and aggregation function to find the median followers based on their joining year
age_groups.select("user_name", "date_joined", "follower_count") \
.distinct().withColumn("post_year", year("date_joined")) \
.groupBy("post_year").agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")) \
.orderBy("post_year") \
.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Find the median follower count of users based on their joining year and age group

# COMMAND ----------

# use age_groups and aggregation function to find the median followers of users based on their joining year and age group
age_groups.select("user_name", "age_group", "date_joined", "follower_count") \
.distinct().withColumn("post_year", year("date_joined")) \
.groupBy("post_year", "age_group").agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")) \
.orderBy("post_year", "age_group") \
.show()

# COMMAND ----------

# Unmount s3 bucket
dbutils.fs.unmount("/mnt/user-129fb6ae3c55-bucket")