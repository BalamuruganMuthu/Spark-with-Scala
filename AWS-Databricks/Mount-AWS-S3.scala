// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Overview
// MAGIC 
// MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in AWS S3. There are two ways to establish access to S3: [IAM roles](https://docs.databricks.com/user-guide/cloud-configurations/aws/iam-roles.html) and access keys.
// MAGIC 
// MAGIC *We recommend using IAM roles to specify which cluster can access which buckets. Keys can show up in logs and table metadata and are therefore fundamentally insecure.* If you do use keys, you'll have to escape the `/` in your keys with `%2F`.
// MAGIC 
// MAGIC This is a **Python** notebook so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` magic command. Python, Scala, SQL, and R are all supported.

// COMMAND ----------

//%fs ls "/FileStore/tables"
dbutils.fs.ls("/FileStore/tables").foreach(println)

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC // File location and type
// MAGIC val file_location = "/FileStore/tables/new_user_credentials.csv"
// MAGIC val file_type = "csv"
// MAGIC 
// MAGIC // CSV options
// MAGIC val infer_schema = "true"
// MAGIC val first_row_is_header = "true"
// MAGIC val delimiter = ","
// MAGIC 
// MAGIC // The applied options are for CSV files. For other file types, these will be ignored.
// MAGIC val df = spark.read.format(file_type)
// MAGIC   .option("inferSchema", infer_schema) 
// MAGIC   .option("header", first_row_is_header) 
// MAGIC   .option("sep", delimiter) 
// MAGIC   .load(file_location)
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions._
// MAGIC 
// MAGIC val ACCESS_KEY = df.where(df("User name") === "databricks-1").select("Access key ID").collect()(0)(0)
// MAGIC val SECRET_KEY = df.where(df("User name") === "databricks-1").select("Secret access key").collect()
// MAGIC 
// MAGIC val EncodedSecretKey = SECRET_KEY(0)(0).toString.replace("/", "%2F")
// MAGIC 
// MAGIC 
// MAGIC println(ACCESS_KEY)

// COMMAND ----------

dbutils.fs.ls("/mnt/")

// COMMAND ----------

val MOUNT_NAME = "bucket-for-databricks"

dbutils.fs.unmount(s"/mnt/$MOUNT_NAME")
dbutils.fs.ls("/mnt/")

// COMMAND ----------

// MAGIC %scala
// MAGIC val AWS_S3_BUCKET = "bucket-for-databricks"
// MAGIC val MOUNT_NAME = "bucket-for-databricks"
// MAGIC 
// MAGIC dbutils.fs.mount(s"s3a://$ACCESS_KEY:$EncodedSecretKey@$AWS_S3_BUCKET", s"/mnt/$MOUNT_NAME")
// MAGIC //display(dbutils.fs.ls(s"/mnt/$MOUNT_NAME"))

// COMMAND ----------

// MAGIC %fs ls "/mnt/bucket-for-databricks"

// COMMAND ----------

val empdf = spark.read.option("header", "false").csv("dbfs:/mnt/bucket-for-databricks/emp_aws.csv").toDF("Number", "Name")

// COMMAND ----------

empdf.show

// COMMAND ----------

// uploading / storing curated data back to S3
empdf.select("Name").write.format("parquet").save("/mnt/bucket-for-databricks/curated1/")

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC //Create a view or table
// MAGIC val temp_table_name = "test"
// MAGIC 
// MAGIC df.createOrReplaceTempView(temp_table_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC /* Query the created temp table in a SQL cell */
// MAGIC 
// MAGIC select * from `test`

// COMMAND ----------

// MAGIC %python
// MAGIC # Since this table is registered as a temp view, it will only be available to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
// MAGIC # Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
// MAGIC # To do so, choose your table name and uncomment the bottom line.
// MAGIC 
// MAGIC permanent_table_name = "{{table_name}}"
// MAGIC 
// MAGIC # df.write.format("{{table_import_type}}").saveAsTable(permanent_table_name)
