# Databricks notebook source
# MAGIC %md #### In-Class Workshop -GR5069

# COMMAND ----------

# MAGIC %md ##### Read Dataset from S3

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date, avg
from pyspark.sql.types import IntegerType

# COMMAND ----------

df_laptimes=spark.read.csv("s3://columbia-gr5069-main/raw/lap_times.csv",header=True)   # copy the S3 URL, allow Spark retrieve data

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_driver=spark.read.csv("s3://columbia-gr5069-main/raw/drivers.csv",header=True)  

# COMMAND ----------

display(df_driver)

# COMMAND ----------

# MAGIC %md #### Transform Data 

# COMMAND ----------

# calculate the age of each driver, create a new column, and then overwrite it  
df_driver=df_driver.withColumn("age",datediff(current_date(),df_driver.dob)/365)

# COMMAND ----------

df_driver = df_driver.withColumn("age", df_driver["age"].cast(IntegerType()))

# COMMAND ----------

display(df_driver)

# COMMAND ----------

df_lap_drivers = df_driver.select('driverId','driverRef', 'forename', 'surname', 'nationality', 'age').join(df_laptimes, on=['driverId'])

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md #### Aggregate by Age

# COMMAND ----------

df_lap_drivers=df_lap_drivers.groupby("age").agg(avg("milliseconds"))

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md #### Load Data into S3

# COMMAND ----------

df_lap_drivers.write.csv("s3://hs-gr5069/processed/in_class_workshop/drivers_laptimes.csv")
