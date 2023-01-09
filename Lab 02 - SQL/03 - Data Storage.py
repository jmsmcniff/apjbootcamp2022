# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Prework, replace all the {username} with firstname_lastname
# MAGIC #### e.g. for David Smith, replace {username} with david_smith

# COMMAND ----------

# DBTITLE 1,Set your personal folder for the test
file_path = "/FileStore/{username}/data/tpch_lineitem"

# COMMAND ----------

dbutils.fs.mkdirs(file_path)

# COMMAND ----------

# DBTITLE 1,Check folder exists and it's empty before the deep clone a table to it
dbutils.fs.ls(file_path)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC drop table if exists lineitem_{username}

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE lineitem_{username}
# MAGIC DEEP CLONE delta.`dbfs:/databricks-datasets/tpch/delta-001/lineitem`
# MAGIC LOCATION "/FileStore/{username}/data/tpch_lineitem"

# COMMAND ----------

# DBTITLE 1,Check the file directory for the newly created table 
display(dbutils.fs.ls(file_path))

# COMMAND ----------

def dirsize(path):
    total=0
    dir_files = dbutils.fs.ls(path)
    for file in dir_files:
        #print(file)
        if file.isDir():
            #print(total)
            total += dirsize(file.path)
        else:
            total += file.size
            #print(file.path+"/ "+str(total))
    return (total)
  
# 1030755031

#file_path = "/tmp/{username}/tpch_lineitem"
print("Current Storage Size is: " + str(round(int(dirsize(file_path))/1024/1024, 2)) + " MB")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### test update a few rows - impact on storage

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from lineitem_{username}
# MAGIC where l_orderkey = 25245506

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### test adding new column with null value - impact on storage

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC update lineitem_{username}
# MAGIC set l_linenumber = 6 -- old value is 3
# MAGIC where l_orderkey = 25245506 and l_partkey = 424134 

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from lineitem_{username}
# MAGIC where l_orderkey = 25245506 and l_partkey = 424134 

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC desc history lineitem_{username}

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC -- sizeInBytes, 1030700217, original size
# MAGIC -- sizeInBytes, 1043542995, new size after update 1 row
# MAGIC DESCRIBE DETAIL "/FileStore/{username}/data/tpch_lineitem"

# COMMAND ----------

# DBTITLE 1,Size after the update rows
# orginal size: 983.0 MB
# update row:   1178.92 MB
print("Current Storage Size is: " + str(round(int(dirsize(file_path))/1024/1024, 2)) + " MB")

# COMMAND ----------

display(dbutils.fs.ls(file_path))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### test adding new column with null value - impact on storage

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC alter table lineitem_{username}
# MAGIC add column new_column int

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from lineitem_{username}

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC desc history lineitem_{username}

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC -- sizeInBytes, 1030700217, original
# MAGIC -- sizeInBytes, 1043542995, updated 1 row
# MAGIC -- sizeInBytes, 1043542995, added a new column, no size change
# MAGIC DESCRIBE DETAIL "/FileStore/{username}/data/tpch_lineitem"

# COMMAND ----------

# orginal size:                 983.0 MB
# update row:                   1178.92 MB
# add column with null value:   1178.92 MB
print("Current Storage Size is: " + str(round(int(dirsize(file_path))/1024/1024, 2)) + " MB")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### test adding new column with not null value - impact on storage

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC alter table lineitem_{username}
# MAGIC add column new_column_not_null int

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from lineitem_{username}

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC desc history lineitem_{username}

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC update lineitem_{username}
# MAGIC set new_column_not_null = 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from lineitem_{username}

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC desc history lineitem_{username}

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- sizeInBytes, 1030700217, original
# MAGIC -- sizeInBytes, 1043542995, updated 1 row
# MAGIC -- sizeInBytes, 1043542995, added a new column, no size change
# MAGIC -- sizeInBytes, 1043542995, added a new_column_not_null 
# MAGIC -- sizeInBytes, 1097542438, updated, created 3 new files 
# MAGIC DESCRIBE DETAIL "/FileStore/{username}/data/tpch_lineitem"

# COMMAND ----------

# orginal size:                 983.0 MB
# update row:                   1178.92 MB
# add column with null value:   1178.92 MB
# add column with value:        2225.68 MB
print("Current Storage Size is: " + str(round(int(dirsize(file_path))/1024/1024, 2)) + " MB")

# COMMAND ----------

display(dbutils.fs.ls(file_path))

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC desc history lineitem_{username}

# COMMAND ----------

# DBTITLE 1,Retain only current version, for demo only don't do this production
# MAGIC %sql 
# MAGIC 
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = False;
# MAGIC 
# MAGIC VACUUM lineitem_{username} RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC desc history lineitem_{username}

# COMMAND ----------

display(dbutils.fs.ls(file_path))

# COMMAND ----------

# orginal size:                           983.0 MB
# update row:                             1178.92 MB
# add column with null value:             1178.92 MB
# add column with value:                  2225.68 MB
# retain only the current version:        1046.83 MB
print("Current Storage Size is: " + str(round(int(dirsize(file_path))/1024/1024, 2)) + " MB")

# COMMAND ----------

# DBTITLE 1,Clean up
dbutils.fs.rm(file_path, True)

# COMMAND ----------


