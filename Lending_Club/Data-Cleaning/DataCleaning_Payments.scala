// Databricks notebook source
// MAGIC %run ../ADLSDBUtils

// COMMAND ----------

// Define the payment schema
val paymentSchema = StructType(List(
      StructField("loan_id", StringType, nullable = false),
      StructField("mem_id", StringType, nullable = false),
      StructField("latest_transaction_id", StringType, nullable = false),
      StructField("funded_amnt_inv", DoubleType, nullable = true),
      StructField("total_pymnt_rec", FloatType, nullable = true),
      StructField("installment", FloatType, nullable = true),
      StructField("last_pymnt_amnt", FloatType, nullable = true),
      StructField("last_pymnt_d", DateType, nullable = true),
      StructField("next_pymnt_d", DateType, nullable = true),
      StructField("pymnt_method", StringType, nullable = true)
                                     )
                              )

// COMMAND ----------

// MAGIC %md #####Read the csv file into a dataframe

// COMMAND ----------

// Read the DataFrame (paymentDf) from  function
val paymentDf: DataFrame = loadDataInDataframe("loan_payment",paymentSchema,"raw/lendingloan", ".csv", "csv", "true", ",")

// COMMAND ----------

// MAGIC %md #####Add the run date to the dataframe

// COMMAND ----------

//Include a run date column to signify when it got ingested into our data lake
val paymentDfRunDate=addRunDate(paymentDf,runDate)

// COMMAND ----------

// MAGIC %md #####Add a surrogate key to the dataframe

// COMMAND ----------

//Include a payment_key column which acts like a surrogate key in the table
val paymentDfKey=paymentDfRunDate.withColumn("payment_key", sha2(concat(col("loan_id"),col("mem_id"),col("latest_transaction_id")), 256))

// COMMAND ----------

// MAGIC %md #####Replace the NULL strings into NULL values

// COMMAND ----------

// List of column names to replace "null" with null
val columnsToReplace = paymentDfKey.columns

// Replace "null" with null values for the specified columns
val paymentNullDf = columnsToReplace.foldLeft(paymentDfKey) { (accDf, colName) =>
  accDf.withColumn(colName, when(col(colName) === "null", lit(null)).otherwise(col(colName)))
}


// COMMAND ----------

// MAGIC %md #####Rename columns in the dataframe

// COMMAND ----------

val paymentData=paymentNullDf.withColumnRenamed("mem_id","member_id") 
                             .withColumnRenamed("funded_amnt_inv","funded_amount_investor") 
                             .withColumnRenamed("total_pymnt_rec","total_payment_recorded") 
                             .withColumnRenamed("last_pymnt_amnt","last_payment_amount") 
                             .withColumnRenamed("last_pymnt_d","last_payment_date") 
                             .withColumnRenamed("next_pymnt_d","next_payment_date") 
                             .withColumnRenamed("pymnt_method","payment_method") 



// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Use Spark SQL to query the data

// COMMAND ----------

paymentData.createOrReplaceTempView("temp_table")
val finalPaymentDf=spark.sql("""select payment_key,run_date,loan_id,member_id,latest_transaction_id,funded_amount_investor,total_payment_recorded, installment,last_payment_amount,last_payment_date,next_payment_date,payment_method from temp_table""")
display(finalPaymentDf)


// COMMAND ----------

// MAGIC %md
// MAGIC #####Write the cleaned dataframe into data lake

// COMMAND ----------

writePartitionDataInParquet("PAYMENT_DETAILS",finalPaymentDf,"work/lendingloan","run_date")


// COMMAND ----------

// MAGIC %sql
// MAGIC msck repair table work.payment_details

// COMMAND ----------

// MAGIC %md ##### Move the input file to archive for future use.

// COMMAND ----------

fileMoveToArchive("loan_payment")

// COMMAND ----------

dbutils.notebook.exit("executed payments job")

// COMMAND ----------


