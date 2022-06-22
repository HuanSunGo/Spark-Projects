# Databricks notebook source
# MAGIC %md ### Q1. What are the average time each driver spent at the pit stop for each race? (10pct)

# COMMAND ----------

from pyspark.sql.functions import *
# from pyspark.sql.types import 

# COMMAND ----------

# read dataset from S3
pit_stops=spark.read.csv("s3://columbia-gr5069-main/raw/pit_stops.csv",header=True)

# show the schema of each table
display(pit_stops)

# COMMAND ----------

# Join the pit_stops file with the driver file, to get the name of driver.
#driver_pit_stop=pit_stops.select('raceId','driverId','duration').join(df_drivers.select('driverId','driverRef'), on=['driverId'])
#display(driver_pit_stop)

# COMMAND ----------

# calculate the average duration, given each driver spent at each race, and order the results in terms of driverId, receId in ascending order
pit_stops=pit_stops.groupby('driverId','raceId').agg(avg('duration'))
pit_stops_avg=pit_stops.orderBy('driverId','raceId')
display(pit_stops_avg)

# COMMAND ----------

# MAGIC %md ### Q2. Rank the average time spent at the pit stop in order of who won each race.(20 pct)

# COMMAND ----------

# import the driver_standings.csv from S3 that includes the wining status of each driver in each race
driver_standings=spark.read.csv("s3://columbia-gr5069-main/raw/driver_standings.csv",header=True)
display(driver_standings)

# COMMAND ----------

# join the driver_standings dataset onto the pit_stop_avg
avg_pit_duration_race=pit_stops_avg.join(driver_standings.select('raceId','driverId','wins'),on=['raceId','driverId'])

display(avg_pit_duration_race)

# COMMAND ----------

# for each race, rank the avg duration at the pit stop, and order by the wining status
avg_pit_duration_race=avg_pit_duration_race.groupby('raceId','wins').agg(avg('avg(duration)'))
avg_pit_duration_race=avg_pit_duration_race.sort('avg(avg(duration))')

# clearly this is not the right answer, wait for further clarification
display(avg_pit_duration_race)

# COMMAND ----------

# MAGIC %md ### Q3. Insert the missing code(e.g: ALO for Alonso) for drivers based on the 'drivers' dataset.(20 pct)

# COMMAND ----------

# import the 'drivers' dataset 
drivers=spark.read.csv("s3://columbia-gr5069-main/raw/drivers.csv",header=True)
display(drivers)

# COMMAND ----------

# check the missing value in each column
df = drivers.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            col(c).contains('\\N') | \  # add one more "\" to avoid the escape 
                            (col(c) == '' ) | \
                            col(c).isNull() | \
                            isnan(c), c 
                           )).alias(c)
                    for c in drivers.columns])
df.show()

# COMMAND ----------

# replace the missing values with the first three characters from the `driverId` column
drivers_code = drivers.withColumn('code', upper(substring('driverRef', 1, 3)))
display(drivers_code)

# check again if the missing value still exists
drivers_code.filter(drivers_code.code == '//N').show()

# COMMAND ----------

# MAGIC %md ### Q4. Who is the youngest and oldest driver for each race? Create a new column called "Age".(20 pct)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Q5.For a given race, which driver has the most wins and losses?(20 pct)

# COMMAND ----------

# MAGIC %md ### Q6. Continue exploring the data by answering your own question. (10 pct)

# COMMAND ----------


