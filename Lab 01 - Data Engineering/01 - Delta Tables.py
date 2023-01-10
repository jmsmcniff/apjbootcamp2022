# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # APJuice Lakehouse Platform
# MAGIC 
# MAGIC <img src="https://github.com/zivilenorkunaite/apjbootcamp2022/blob/main/images/APJuiceLogo.png?raw=true" style="width: 650px; max-width: 100%; height: auto" />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Introduction
# MAGIC 
# MAGIC In this Notebook we will see how to work with Delta Tables when using Databricks Notebooks. 
# MAGIC 
# MAGIC Some of the things we will look at are:
# MAGIC * Creating a new Delta Table
# MAGIC * Using Delta Log and Time Traveling 
# MAGIC * Tracking data changes using Change Data Feed
# MAGIC * Cloning tables
# MAGIC * Masking data by using Dynamic Views
# MAGIC * 
# MAGIC 
# MAGIC In addition to Delta Tables we will also get to see some tips and tricks on working on Databricks environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ### APJ Data Sources
# MAGIC 
# MAGIC For this exercise we will be starting to implement Lakehouse platform for our company, APJuice.
# MAGIC 
# MAGIC APJuice has been running for a while and we already had multiple data sources that could be used. To begin with, we have decided to focus on sales transactions that are uploaded from our store locations directly to cloud storage account. In addition to sales data we already had couple of dimension tables that we have exported to files and uploaded to cloud storage as well.
# MAGIC 
# MAGIC For the first part of the exercise we will be focusing on an export of Store Locations table that has been saved as `csv` file.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Environment Setup
# MAGIC 
# MAGIC We will be using [Databricks Notebooks workflow](https://docs.databricks.com/notebooks/notebook-workflows.html) element to set up environment for this exercise. 
# MAGIC 
# MAGIC `dbutils.notebook.run()` command will run another notebook and return its output to be used here.
# MAGIC 
# MAGIC `dbutils` has some other interesting uses such as interacting with file system or reading [Databricks Secrets](https://docs.databricks.com/dev-tools/databricks-utils.html#dbutils-secrets)

# COMMAND ----------

setup_responses = dbutils.notebook.run("./Utils/Setup-Batch", 0).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

print("Local data path is {}".format(local_data_path))
print("DBFS path is {}".format(dbfs_data_path))
print("Database name is {}".format(database_name))

spark.sql(f"USE {database_name};")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta Tables
# MAGIC 
# MAGIC Let's load store locations data to Delta Table. In our case we don't want to track any history and opt to overwrite data every time process is running.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create Delta Table
# MAGIC 
# MAGIC ***Load Store Locations data to Delta Table***
# MAGIC 
# MAGIC In our example CRM export has been provided to us as a CSV file and uploaded to `dbfs_data_path` location. It could also be your S3 bucket, Azure Storage account or Google Cloud Storage. 
# MAGIC 
# MAGIC We will not be looking at how to set up access to files on the cloud environment in today's workshop.
# MAGIC 
# MAGIC 
# MAGIC For our APJ Data Platform we know that we will not need to keep and manage history for this data so creating table can be a simple overwrite each time ETL runs.
# MAGIC 
# MAGIC 
# MAGIC Let's start with simply reading CSV file into DataFrame with no upfront schema definition needed

# COMMAND ----------

dataPath = f"{dbfs_data_path}/stores.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("quote", "\"") \
  .option("inferSchema", "true")\
  .csv(dataPath)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Data is in a DataFrame, but not yet in a Delta Table. Still, we can already use SQL to query data or copy it into the Delta table

# COMMAND ----------

# Creating a Temporary View will allow us to use SQL to interact with data

df.createOrReplaceTempView("stores_csv_file")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * from stores_csv_file

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC SQL DDL can be used to create table using view we have just created. 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS stores;
# MAGIC 
# MAGIC CREATE TABLE stores
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM stores_csv_file;
# MAGIC 
# MAGIC SELECT * from stores;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This SQL query has created a simple Delta Table (as specified by `USING DELTA`). DELTA a default format so it would create a delta table even if we skip the `USING DELTA` part.
# MAGIC 
# MAGIC For more complex tables you can also specify table PARTITION or add COMMENTS.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Describe Delta Table
# MAGIC 
# MAGIC Now that we have created our first Delta Table - let's see what it looks like on our database and where are the data files stored.  
# MAGIC 
# MAGIC Quick way to get information on your table is to run `DESCRIBE EXTENDED` command on SQL cell

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from stores

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED stores

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC One of the rows in output above provides us with `Location` information. This is where the actual delta files are being stored.
# MAGIC 
# MAGIC Now that we know the location of this table, let's look at the files! We can use another `dbutils` command for that

# COMMAND ----------

table_location = f"dbfs:/user/hive/warehouse/{database_name}.db/stores"

displayHTML(f"""Make sure <b style="color:green">{table_location}</b> match your table location as per <b>DESCRIBE EXTENDED</b> output above""")

dbutils.fs.ls(table_location)

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY stores;

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Update Delta Table
# MAGIC 
# MAGIC Provided dataset has address information, but no country name - let's add one!

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC alter table stores
# MAGIC add column store_country string;

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC update stores
# MAGIC set store_country = case when id in ('SYD01', 'MEL01', 'BNE02','CBR01','PER01') then 'AUS' when id in ('AKL01', 'AKL02', 'WLG01') then 'NZL' end

# COMMAND ----------

# MAGIC %sql 
# MAGIC select store_country, id, name from stores

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update stores
# MAGIC set store_country = 'AUS'
# MAGIC where id = 'MEL02'

# COMMAND ----------

# MAGIC %sql
# MAGIC select store_country, count(id) as number_of_stores from stores group by store_country

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Track Data History
# MAGIC 
# MAGIC 
# MAGIC Delta Tables keep all changes made in the delta log we've seen before. There are multiple ways to see that - e.g. by running `DESCRIBE HISTORY` for a table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY stores

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Having all this information and old data files mean that we can **Time Travel**!  You can query your table at any given `VERSION AS OF` or  `TIMESTAMP AS OF`.
# MAGIC 
# MAGIC Let's check again what table looked like before we ran last update

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from stores VERSION AS OF 2 where id = 'MEL02';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can remove files no longer referenced by a Delta table and are older than the retention threshold by running the `VACCUM` command on the table. vacuum is not triggered automatically. The default retention threshold for the files is 7 days.
# MAGIC 
# MAGIC `vacuum` deletes only data files, not log files. Log files are deleted automatically and asynchronously after checkpoint operations. The default retention period of log files is 30 days, configurable through the `delta.logRetentionDuration` property which you set with the `ALTER TABLE SET TBLPROPERTIES` SQL method.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC VACUUM stores

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from stores VERSION AS OF 2 
# MAGIC where id = 'MEL02';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY stores

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC ALTER TABLE stores
# MAGIC SET TBLPROPERTIES (delta.logRetentionDuration = '180 Days')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Change Data Feed
# MAGIC 
# MAGIC 
# MAGIC The Delta change data feed represents row-level changes between versions of a Delta table. When enabled on a Delta table, the runtime records “change events” for all the data written into the table. This includes the row data along with metadata indicating whether the specified row was inserted, deleted, or updated.
# MAGIC 
# MAGIC It is not enabled by default, but we can enabled it using `TBLPROPERTIES`

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE stores SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Changes to table properties also generate a new version

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe history stores

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Change data feed can be seen by using `table_changes` function. You will need to specify a range of changes to be returned - it can be done by providing either version or timestamp for the start and end. The start and end versions and timestamps are inclusive in the queries. 
# MAGIC 
# MAGIC To read the changes from a particular start version to the latest version of the table, specify only the starting version or timestamp.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- simulate change of address for store AKL01 and removal of store BNE02
# MAGIC 
# MAGIC update stores
# MAGIC set hq_address = 'Domestic Terminal, AKL'
# MAGIC where id = 'AKL01';
# MAGIC 
# MAGIC delete from stores
# MAGIC where id = 'BNE02';
# MAGIC 
# MAGIC SELECT * FROM table_changes('stores', 8, 9) -- Note that we increment versions due to UPDATE statements above

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Delta CDC gives back 4 cdc types in the "__change_type" column:
# MAGIC 
# MAGIC | CDC Type             | Description                                                               |
# MAGIC |----------------------|---------------------------------------------------------------------------|
# MAGIC | **update_preimage**  | Content of the row before an update                                       |
# MAGIC | **update_postimage** | Content of the row after the update (what you want to capture downstream) |
# MAGIC | **delete**           | Content of a row that has been deleted                                    |
# MAGIC | **insert**           | Content of a new row that has been inserted                               |
# MAGIC 
# MAGIC Therefore, 1 update results in 2 rows in the cdc stream (one row with the previous values, one with the new values)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### CLONE
# MAGIC 
# MAGIC 
# MAGIC What if our use case is more of a having monthly snapshots of the data instead of detailed changes log? Easy way to get it done is to create CLONE of table.
# MAGIC 
# MAGIC You can create a copy of an existing Delta table at a specific version using the clone command. Clones can be either deep or shallow.
# MAGIC 
# MAGIC  
# MAGIC 
# MAGIC * A **deep clone** is a clone that copies the source table data to the clone target in addition to the metadata of the existing table.
# MAGIC * A **shallow clone** is a clone that does not copy the data files to the clone target. The table metadata is equivalent to the source. These clones are cheaper to create, but they will break if original data files were not available

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists stores_clone;
# MAGIC 
# MAGIC create table stores_clone DEEP CLONE stores VERSION AS OF 3 -- you can specify timestamp here instead of a version

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe history stores_clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists stores_clone_shallow;
# MAGIC 
# MAGIC -- Note that no files are copied
# MAGIC 
# MAGIC create table stores_clone_shallow SHALLOW CLONE stores

# COMMAND ----------

# MAGIC %md ##  ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Use Schema Enforcement to protect data quality

# COMMAND ----------

# MAGIC %md To show you how schema enforcement works, let's load in a much larger table (1,000,000 rows) which has two additional columns -- `store_description` and `store_inception_date` -- that do not match our existing Delta Lake table schema.

# COMMAND ----------

dataPath = f"{dbfs_data_path}/StoresLarge.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .option("quote", "\"") \
  .csv(dataPath)

df.createOrReplaceTempView("new_stores_csv")
df.printSchema()


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS new_stores;
# MAGIC 
# MAGIC CREATE TABLE new_stores
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM new_stores_csv;
# MAGIC 
# MAGIC SELECT * from new_stores;

# COMMAND ----------

# MAGIC %sql DESCRIBE extended new_stores

# COMMAND ----------

# Uncommenting this cell will lead to an error because the schemas don't match.
# Attempt to write data with new column to Delta Lake table
# spark.table("new_stores").limit(5000).write.format("delta").mode("append").saveAsTable("stores")

# COMMAND ----------

# MAGIC %md **Schema enforcement helps keep our tables clean and tidy so that we can trust the data we have stored in Delta Lake.** The writes above were blocked because the schema of the new data did not match the schema of table (see the exception details). See more information about how it works [here](https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html).
# MAGIC 
# MAGIC `store_description` and `store_inception_date` are new columns, and `store_country` is missing!

# COMMAND ----------

# MAGIC %md ##  ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Use Schema Evolution to add new columns to schema
# MAGIC 
# MAGIC If we *want* to update our Delta Lake table to match this data source's schema, we can do so using schema evolution. Simply add the following to the Spark write command: `.option("mergeSchema", "true")`

# COMMAND ----------

spark.table("new_stores").limit(5000).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("stores")

# COMMAND ----------

# MAGIC %md
# MAGIC # Take a look at the data now!
# MAGIC 
# MAGIC You will see the following:
# MAGIC - Two new columns added, with null values for the older data, and completed values where ingested from the new_stores dataset (the record with id JME....)
# MAGIC - null values for the new record in the `store_country` column since this column is not present in the new_stores data.

# COMMAND ----------

# MAGIC %sql SELECT * FROM stores WHERE id IN ('MEL02', 'JME131338302')

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Support Change Data Capture Workflows & Other Ingest Use Cases via `MERGE INTO`

# COMMAND ----------

# MAGIC %md
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif" alt='Merge process' width=600/>
# MAGIC 
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake
# MAGIC 
# MAGIC 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use `MERGE`

# COMMAND ----------

# View delta files before UPDATE
table_location = f"dbfs:/user/hive/warehouse/{database_name}.db/new_stores"

displayHTML(f"""Make sure <b style="color:green">{table_location}</b> match your table location as per <b>DESCRIBE EXTENDED</b> output above""")

display(dbutils.fs.ls(table_location))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update a few records inside the new stores table
# MAGIC UPDATE new_stores SET city = 'Canberra' where id='RZR428231860';
# MAGIC UPDATE new_stores SET city = 'Hobart' where id='AAM498823732'

# COMMAND ----------

# View delta files after UPDATE
table_location = f"dbfs:/user/hive/warehouse/{database_name}.db/new_stores"

displayHTML(f"""Make sure <b style="color:green">{table_location}</b> match your table location as per <b>DESCRIBE EXTENDED</b> output above""")

display(dbutils.fs.ls(table_location))

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled = true;
# MAGIC MERGE INTO stores AS s
# MAGIC USING new_stores AS n
# MAGIC ON s.id = n.id
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled = false;

# COMMAND ----------

# MAGIC %sql SELECT * FROM stores WHERE id IN ('PER01', 'RZR428231860', 'AAM498823732')

# COMMAND ----------

# MAGIC %md ### <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> Cache table in memory (Databricks Delta Lake only)

# COMMAND ----------

# MAGIC %sql CACHE SELECT * FROM stores

# COMMAND ----------

# MAGIC %md ### <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> Z-Order Optimize (Databricks Delta Lake only)

# COMMAND ----------

# MAGIC %sql OPTIMIZE stores ZORDER BY id

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Dynamic Views
# MAGIC 
# MAGIC 
# MAGIC Our stores table has some PII data (email, phone number). We can use dynamic views to limit visibility to the columns and rows depending on groups user belongs to.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from stores

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS v_stores_email_redacted;
# MAGIC 
# MAGIC CREATE VIEW v_stores_email_redacted AS
# MAGIC SELECT
# MAGIC   id,
# MAGIC   name,
# MAGIC   CASE WHEN
# MAGIC     is_member('admins') THEN email
# MAGIC     ELSE 'REDACTED'
# MAGIC   END AS email,
# MAGIC   city,
# MAGIC   hq_address,
# MAGIC   phone_number
# MAGIC FROM stores;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from v_stores_email_redacted;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS v_stores_country_limited;
# MAGIC 
# MAGIC CREATE VIEW v_stores_country_limited AS
# MAGIC SELECT *
# MAGIC FROM stores
# MAGIC WHERE 
# MAGIC   (is_member('admins') OR id = 'SYD01');

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from v_stores_country_limited;
