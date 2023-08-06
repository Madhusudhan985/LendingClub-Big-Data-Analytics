// Databricks notebook source
// MAGIC %run ../ADLSDBUtils

// COMMAND ----------

//Infer the schema of the loan Defaulter's data

val loanDefaultersSchema = StructType(List(
                                        StructField("loan_id", StringType, false),
                                        StructField("mem_id", StringType, false),
                                        StructField("def_id", StringType, false),
                                        StructField("delinq_2yrs", IntegerType, true),
                                        StructField("delinq_amnt", FloatType, true),
                                        StructField("pub_rec", IntegerType, true),
                                        StructField("pub_rec_bankruptcies", IntegerType, true),
                                        StructField("inq_last_6mths", IntegerType, true),
                                        StructField("total_rec_late_fee", FloatType, true),
                                        StructField("hardship_flag", StringType, true),
                                        StructField("hardship_type", StringType, true),
                                        StructField("hardship_length", IntegerType, true),
                                        StructField("hardship_amount", FloatType, true)
                                      )
                                   )


// COMMAND ----------

// MAGIC %md
// MAGIC #####Read the csv file into a dataframe

// COMMAND ----------

val defaultersDf = loadDataInDataframe("loan_defaulters",loanDefaultersSchema, "raw/lendingloan", ".csv", "csv", "true", ",")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Add the run date to the dataframe

// COMMAND ----------

//Include a run date column to signify when it got ingested into our data lake
val defaultersDfRunDate=addRunDate(defaultersDf,runDate)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Add a surrogate key to the dataframe
// MAGIC

// COMMAND ----------

//Include a loan_default_key column which acts like a surrogate key in the table
val defaultersDfKey=defaultersDfRunDate.withColumn("loan_default_key", sha2(concat(col("loan_id"),col("mem_id"),col("def_id")), 256))

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Replace the NULL strings into NULL values

// COMMAND ----------


// List of column names to replace "null" with null
val columnsToReplace = defaultersDfKey.columns

// Replace "null" with null values for the specified columns
val defaultersData = columnsToReplace.foldLeft(defaultersDfKey) { (accDf, colName) =>
  accDf.withColumn(colName, when(col(colName) === "null", lit(null)).otherwise(col(colName)))
}


// COMMAND ----------

// MAGIC %md #####Rename the columns to a better understandable way

// COMMAND ----------

val renamedDefaultersDf=defaultersData.withColumnRenamed("mem_id", "member_id") 
                                      .withColumnRenamed("def_id", "loan_default_id") 
                                      .withColumnRenamed("delinq_2yrs", "defaulters_2yrs") 
                                      .withColumnRenamed("delinq_amnt", "defaulters_amount") 
                                      .withColumnRenamed("pub_rec", "public_records") 
                                      .withColumnRenamed("pub_rec_bankruptcies", "public_records_bankruptcies") 
                                      .withColumnRenamed("inq_last_6mths", "enquiries_6mnths") 
                                      .withColumnRenamed("total_rec_late_fee", "late_fee") 


// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Use Spark SQL to query the data

// COMMAND ----------

renamedDefaultersDf.createOrReplaceTempView("temp")
val finalDefaultersDf=spark.sql("""select loan_default_key, run_date, loan_id,member_id,loan_default_id,defaulters_2yrs,defaulters_amount,public_records,public_records_bankruptcies,enquiries_6mnths,late_fee,hardship_flag,hardship_type,hardship_length,hardship_amount from temp""")
//display(finalDefaultersDf)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Write the cleaned dataframe into data lake

// COMMAND ----------

writePartitionDataInParquet("DEFAULTER_DETAILS",finalDefaultersDf,"work/lendingloan","run_date")

// COMMAND ----------

// DBTITLE 1,Move the input file to archive for future use.
fileMoveToArchive("loan_defaulters")

// COMMAND ----------

dbutils.notebook.exit("executed loan defaulters job")
