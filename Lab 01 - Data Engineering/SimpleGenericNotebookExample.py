# Databricks notebook source
# MAGIC %md
# MAGIC A simple generic notebook example of loading data from a specified path that you can pass at run time from a job or another notebook. 
# MAGIC 
# MAGIC You can also change the widgets manually at the top of the page if you wish

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("database_name", "james_mcniff_parameter_demo")
dbutils.widgets.text("dataset_name", "taxi_payment_type")
dbutils.widgets.text("dataset_path", "/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv")
dbutils.widgets.text("csv_delimiter", ",")

# COMMAND ----------

database_name = dbutils.widgets.get("database_name")
dataset_name = dbutils.widgets.get("dataset_name")
dataset_path = dbutils.widgets.get("dataset_path")
csv_delimiter = dbutils.widgets.get("csv_delimiter")

# COMMAND ----------

print(f"Database name: {database_name}")
print(f"Dataset name: {dataset_name}")
print(f"Dataset path: {dataset_path}")
print(f"CSV Delimiter: {csv_delimiter}")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name};")

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_${dataset_name} USING CSV OPTIONS (
# MAGIC   path = "${dataset_path}",
# MAGIC   header = "true",
# MAGIC   inferSchema = "true",
# MAGIC   sep = "${csv_delimiter}",
# MAGIC   quote = "\"",
# MAGIC   mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
# MAGIC );

# COMMAND ----------

# MAGIC %sql select * from temp_${dataset_name} 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze_${dataset_name};
# MAGIC 
# MAGIC CREATE TABLE bronze_${dataset_name}
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ("delta.columnMapping.mode" = "name")
# MAGIC AS
# MAGIC SELECT * FROM temp_${dataset_name};
# MAGIC 
# MAGIC SELECT * from bronze_${dataset_name};

# COMMAND ----------

# MAGIC %sql select count(*) from bronze_${dataset_name};

# COMMAND ----------

# display(dbutils.fs.ls('/databricks-datasets/bikeSharing/data-001/'))

# COMMAND ----------


