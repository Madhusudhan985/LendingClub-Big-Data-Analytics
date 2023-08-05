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

// DBTITLE 1,Read the csv file into a dataframe
val customerDf = loadDataInDataframe("loan_customer_data",customerSchema, "raw_data", ".csv", "csv", "true", ",")

// COMMAND ----------

// DBTITLE 1,Rename the columns to a better understandable way
val renamedCustomerDf=customerDf.withColumnRenamed("cust_id","customer_id")
                            .withColumnRenamed("mem_id","member_id")
                            .withColumnRenamed("fst_name","first_name")
                            .withColumnRenamed("lst_name","last_name")
                            .withColumnRenamed("prm_status","premium_status")


// COMMAND ----------

// DBTITLE 1,Add the run date to the dataframe
//Include a run date column to signify when it got ingested into our data lake
val customerDfRunDate=renamedCustomerDf.withColumn("run_date", lit(runDate)) 

// COMMAND ----------

// DBTITLE 1,Add a surrogate key to the dataframe
//Include a customer_key column which acts like a surrogate key in the table
//SHA-2 (Secure Hash Algorithm 2) is a set of cryptographic hash functions. It produces a 256-bit (32-byte) hash value and is generally considered to be a more secure.
val customerData=customerDfRunDate.withColumn("customer_key", sha2(concat(col("member_id"),col("age"),col("state")), 256))

// COMMAND ----------

// DBTITLE 1,Move the input file to archive for future use.
fileMoveToArchive("loan_customer_data")

// COMMAND ----------

// DBTITLE 1,Write the cleaned dataframe into data lake
//write the final cleaned customers data to data lake
//display_df.write.options(header='True').mode("append").parquet("/mnt/datasetbigdata/processed-data/lending_loan/customer_details")
