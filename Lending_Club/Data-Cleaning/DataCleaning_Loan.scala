// Databricks notebook source
// MAGIC %run ../ADLSDBUtils

// COMMAND ----------

//Infer the schema of the loan's data
val loanSchema = StructType(List(
                                     StructField("loan_id", StringType, false),
                                     StructField("mem_id", StringType, false),
                                     StructField("acc_id", StringType, false),
                                     StructField("loan_amt", DoubleType, true),
                                     StructField("fnd_amt", DoubleType, true),
                                     StructField("term", StringType, true),
                                     StructField("interest", StringType, true),
                                     StructField("installment", FloatType, true),
                                     StructField("issue_date", DateType, true),
                                     StructField("loan_status", StringType, true),
                                     StructField("purpose", StringType, true),
                                     StructField("title", StringType, true),
                                     StructField("disbursement_method", StringType, true)
                                  )  
                            )

// COMMAND ----------

// MAGIC %md #####Read the csv file into a dataframe

// COMMAND ----------

val loanDf = loadDataInDataframe("loan_details",loanSchema, "raw/lendingloan", ".csv", "csv", "true", ",")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Cleaning Techniques to include

// COMMAND ----------

// Define the strings to remove
val stringsToRemove = Map(
      "term" -> "months",
      "interest" -> "%"
    )

// Apply string removal using a loop
var cleanedDf = loanDf
for ((colName, stringToRemove) <- stringsToRemove) {
      cleanedDf = cleanedDf.withColumn(colName, regexp_replace(col(colName), stringToRemove, ""))
  }


// COMMAND ----------

// MAGIC %md
// MAGIC ##### Rename columns in the dataframe

// COMMAND ----------

val renamedLoanDf=cleanedDf.withColumnRenamed("mem_id","member_id") 
                           .withColumnRenamed("acc_id","account_id") 
                           .withColumnRenamed("loan_amt","loan_amount") 
                           .withColumnRenamed("fnd_amt","funded_amount") 

// COMMAND ----------

// MAGIC %md #####Add the run date to the dataframe

// COMMAND ----------

//Include a run date column to signify when it got ingested into our data lake
val loanDfRunDate=addRunDate(renamedLoanDf,runDate)

// COMMAND ----------

// MAGIC %md ##### Add a surrogate key to the dataframe

// COMMAND ----------

//Include a loan_key column which acts like a surrogate key in the table
val loanDfKey=loanDfRunDate.withColumn("loan_key", sha2(concat(col("loan_id"),col("member_id"),col("loan_amount")), 256))


// COMMAND ----------

// MAGIC %md ##### Replace the NULL strings into NULL values

// COMMAND ----------


// List of column names to replace "null" with null
val columnsToReplace = loanDfKey.columns

// Replace "null" with null values for the specified columns
val loanData = columnsToReplace.foldLeft(loanDfKey) { (accDf, colName) =>
  accDf.withColumn(colName, when(col(colName) === "null", lit(null)).otherwise(col(colName)))
}


// COMMAND ----------

// MAGIC %md ##### Use Spark SQL to query the data

// COMMAND ----------

loanData.createOrReplaceTempView("temp_table")
val finalLoanDf=spark.sql("""select loan_key, run_date,loan_id,member_id,account_id,loan_amount,funded_amount,term,interest,installment,issue_date,loan_status,purpose,title,disbursement_method from temp_table""")
//display(finalLoanDf)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Write the cleaned dataframe into data lake

// COMMAND ----------

writePartitionDataInParquet("LOAN_DETAILS",finalLoanDf,"work/lendingloan","run_date")

// COMMAND ----------

// DBTITLE 1,Move the input file to archive for future use.
fileMoveToArchive("loan_details")

// COMMAND ----------

dbutils.notebook.exit("executed loan job")
