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

// DBTITLE 1,Read the csv file into a dataframe
val loanDf = loadDataInDataframe("loan_details",loanSchema, "raw_data", ".csv", "csv", "true", ",")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 03: Cleaning Techniques to include

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
// MAGIC ### Rename columns in the dataframe

// COMMAND ----------

val renamedLoanDf=cleanedDf.withColumnRenamed("mem_id","member_id") 
                           .withColumnRenamed("acc_id","account_id") 
                           .withColumnRenamed("loan_amt","loan_amount") 
                           .withColumnRenamed("fnd_amt","funded_amount") 

// COMMAND ----------

// DBTITLE 1,Add the run date to the dataframe
//Include a run date column to signify when it got ingested into our data lake
val loanDfRunDate=addRunDate(renamedLoanDf,runDate)

// COMMAND ----------

// DBTITLE 1,Add a surrogate key to the dataframe
//Include a loan_key column which acts like a surrogate key in the table
val loanDfKey=loanDfRunDate.withColumn("loan_key", sha2(concat(col("loan_id"),col("member_id"),col("loan_amount")), 256))


// COMMAND ----------

// DBTITLE 1,Replace the NULL strings into NULL values

// List of column names to replace "null" with null
val columnsToReplace = loanDfKey.columns

// Replace "null" with null values for the specified columns
val loanData = columnsToReplace.foldLeft(loanDfKey) { (accDf, colName) =>
  accDf.withColumn(colName, when(col(colName) === "null", lit(null)).otherwise(col(colName)))
}


// COMMAND ----------

// DBTITLE 1,Move the input file to archive for future use.
fileMoveToArchive("loan_details")

// COMMAND ----------

// DBTITLE 1,Write the cleaned dataframe into data lake
//write the final cleaned customers data to data lake
//display_df.write.options(header='True').mode("append").parquet("/mnt/datasetbigdata/processed-data/lending_loan/customer_details")
