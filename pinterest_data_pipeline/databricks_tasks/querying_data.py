# Databricks notebook source
# MAGIC %md
# MAGIC # Querying the data

# COMMAND ----------

# Run after mount_s3_load_data with clean_data appended 

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
