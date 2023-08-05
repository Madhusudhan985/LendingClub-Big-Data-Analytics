// Databricks notebook source
// MAGIC %run ../ADLSDBUtils

// COMMAND ----------

//Infer the schema of the customer's data
val customerSchema = StructType(List(
                    StructField("cust_id", StringType, true), //True indicates it contains null values.
                    StructField("mem_id", StringType, true),
                    StructField("fst_name", StringType, false),
                    StructField("lst_name", StringType, false),
                    StructField("prm_status", StringType, false),
                    StructField("age", IntegerType, false),
                    StructField("state", StringType, false),
                    StructField("country",StringType, false)
                    ))

// COMMAND ----------

// MAGIC %md ##### Read the csv file into a dataframe

// COMMAND ----------

val customerDf = loadDataInDataframe("loan_customer_data",customerSchema, "raw", ".csv", "csv", "true", ",")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Rename columns in the dataframe

// COMMAND ----------

val renamedCustomerDf=customerDf.withColumnRenamed("cust_id","customer_id")
                            .withColumnRenamed("mem_id","member_id")
                            .withColumnRenamed("fst_name","first_name")
                            .withColumnRenamed("lst_name","last_name")
                            .withColumnRenamed("prm_status","premium_status")


// COMMAND ----------

// MAGIC %md ##### Add the run date to the dataframe

// COMMAND ----------

//Include a run date column to signify when it got ingested into our data lake
val customerDfRunDate=addRunDate(renamedCustomerDf,runDate)

// COMMAND ----------

// MAGIC %md ##### Add a surrogate key to the dataframe

// COMMAND ----------

//Include a customer_key column which acts like a surrogate key in the table
//SHA-2 (Secure Hash Algorithm 2) is a set of cryptographic hash functions. It produces a 256-bit (32-byte) hash value and is generally considered to be a more secure.
val customerData=customerDfRunDate.withColumn("customer_key", sha2(concat(col("member_id"),col("age"),col("state")), 256))

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Use Spark SQL to query the data

// COMMAND ----------


customerData.createOrReplaceTempView("temp_table")
val finalCustomerDf=spark.sql("""select customer_key,run_date,customer_id,member_id,first_name,last_name,premium_status,age,state,country from temp_table""")

// COMMAND ----------

// MAGIC %md #####Write the cleaned dataframe into data lake

// COMMAND ----------

writePartitionDataInParquetAndCreateTable("customers","work",finalCustomerDf,"work","run_date")


// COMMAND ----------

// MAGIC %md #####Move the input file to archive for future use.

// COMMAND ----------

fileMoveToArchive("loan_customer_data")

// COMMAND ----------

dbutils.notebook.exit("executed customers job")
