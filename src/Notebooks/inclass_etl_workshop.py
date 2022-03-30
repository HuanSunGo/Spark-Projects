# Databricks notebook source
# MAGIC %md #### In-Class Workshop -GR5069

# COMMAND ----------

# MAGIC %md Read Dataset from S3

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date, avg
from pyspark.sql.types import IntegerType

# COMMAND ----------

df_laptimes=spark.read.csv("s3://columbia-gr5069-main/raw/lap_times.csv",header=True,inferSchema=True)   
# copy the S3 URL, allow Spark retrieve data

# COMMAND ----------

# MAGIC %md Check the schema of dataframe, not desirable types of having all strings. --> `inferSchema` to automatically choose the datatype.
# MAGIC Otherwise should `cast` manually.

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_driver=spark.read.csv("s3://columbia-gr5069-main/raw/drivers.csv",header=True,inferSchema=True)  
display(df_driver)

# COMMAND ----------

# check why that the `number` column is STRING. --> because there's `\N` in the column, and counts for 804 values.
display(df_driver.groupby('number').count())

# COMMAND ----------

df_driver_count=df_driver.select('driverId').count()
df_driver_count

# COMMAND ----------

df_driver.count()

# COMMAND ----------

df_driver_count=df_driver.select('driverId').distinct().count()
df_driver_count

# COMMAND ----------

# MAGIC %md The previous 3 steps shows that `drivers` is clean.

# COMMAND ----------

# MAGIC %md ### 02-23-2022 Class 6

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

# COMMAND ----------

# MAGIC %md ### 03-30-2022 Class 10
# MAGIC In class workshop 

# COMMAND ----------

import boto3 # use the data from s3
import pandas as pd

s3 = boto3.client('s3')

bucket = "ne-gr5069"
airbnb_data = "raw/sf-listings/airbnb-cleaned-mlflow.csv"

obj_laps = s3.get_object(Bucket= bucket, Key= airbnb_data) 
df = pd.read_csv(obj_laps['Body'])

# COMMAND ----------

display(df)

# COMMAND ----------

from io import StringIO # python3; python2: BytesIO 
import boto3

# load pandas file directly into S3 
# previously was write spark df into S3 by using df.write.csv
bucket = 'hs-gr5069' # already created on S3
csv_buffer = StringIO()
df.to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, 'df.csv').put(Body=csv_buffer.getvalue())

# COMMAND ----------

bucket = "hs-gr5069"
airbnb_data = "df.csv"

obj = s3.get_object(Bucket= bucket, Key= airbnb_data) 
df_airbnb = pd.read_csv(obj['Body'])

# COMMAND ----------

display(df_airbnb)

# COMMAND ----------

df_airbnb=df_airbnb.drop(['_c0','Unnamed: 0'],axis=1)
df_airbnb.head()

# COMMAND ----------

df_airbnb.columns

# COMMAND ----------


