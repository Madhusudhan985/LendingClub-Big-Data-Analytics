// Databricks notebook source
// MAGIC %run ../ADLSDBUtils

// COMMAND ----------

//Define accounts data Schema
val accountSchema = StructType(List(
                                     StructField("acc_id", StringType, false),
                                     StructField("mem_id", StringType, false),
                                     StructField("loan_id", StringType, false),
                                     StructField("grade", StringType, true),
                                     StructField("sub_grade",StringType, true),
                                     StructField("emp_title",StringType, true),
                                     StructField("emp_length",StringType, true),
                                     StructField("home_ownership",StringType, true),
                                     StructField("annual_inc",FloatType, true),
                                     StructField("verification_status",StringType, true),
                                     StructField("tot_hi_cred_lim",FloatType, true),
                                     StructField("application_type",StringType, true),
                                     StructField("annual_inc_joint",StringType, true),
                                     StructField("verification_status_joint",StringType, true)
                                    
                                     )
                                  )

// COMMAND ----------

// MAGIC %md
// MAGIC ###Read the csv file into a dataframe

// COMMAND ----------

val accountDf = loadDataInDataframe("account_details",accountSchema, "raw", ".csv", "csv", "true", ",")

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Optimized way to clean your dataframe having extra strings and symbols from a dataframe column

// COMMAND ----------

val cleanAccountDf = accountDf
  .withColumn("emp_length", regexp_replace(col("emp_length"), "[^0-9]", " "))
  .withColumn("emp_length", col("emp_length").cast("integer"))
  .withColumn("emp_length", when(col("emp_length") === "null", lit(null)).otherwise(col("emp_length"))) //Replace the NULL strings into NULL values


// COMMAND ----------

// MAGIC %md
// MAGIC ### Add the run date to the dataframe

// COMMAND ----------

//Include a run date column to signify when it got ingested into our data lake
val accountDfRunDate=addRunDate(cleanAccountDf,runDate)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Add a surrogate key to the dataframe
// MAGIC

// COMMAND ----------

//Include a account_key column which acts like a surrogate key in the table
val accountDfKey=accountDfRunDate.withColumn("account_key", sha2(concat(col("acc_id"),col("mem_id"),col("loan_id")), 256))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Rename columns in the dataframe

// COMMAND ----------

val accountDfRename=accountDfKey.withColumnRenamed("acc_id","account_id") 
                                .withColumnRenamed("mem_id","member_id") 
                                .withColumnRenamed("emp_title","employee_designation") 
                                .withColumnRenamed("emp_length","employee_experience") 
                                .withColumnRenamed("annual_inc","annual_income") 
                                .withColumnRenamed("tot_hi_cred_lim","total_high_credit_limit") 
                                .withColumnRenamed("annual_inc_joint","annual_income_joint") 



// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Use Spark SQL to query the data

// COMMAND ----------

accountDfRename.createOrReplaceTempView("temp_table")
val finalAccountDf=spark.sql("""select account_key,run_date,account_id,member_id,loan_id,grade,sub_grade,employee_designation,employee_experience,home_ownership,annual_income,verification_status,total_high_credit_limit,application_type,annual_income_joint,verification_status_joint from temp_table """)


// COMMAND ----------

// MAGIC %md
// MAGIC #####Write the cleaned dataframe into data lake

// COMMAND ----------

writePartitionDataInParquetAndCreateTable("accounts","work",finalAccountDf,"work","run_date")


// COMMAND ----------

// MAGIC %md
// MAGIC ###Move the input file to archive for future use.

// COMMAND ----------

fileMoveToArchive("loan_customer_data")
