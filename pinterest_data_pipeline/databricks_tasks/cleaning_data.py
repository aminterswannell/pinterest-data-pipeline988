# Databricks notebook source
# Appended to mount_s3_load_data notebook and run to clean dataframes

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
