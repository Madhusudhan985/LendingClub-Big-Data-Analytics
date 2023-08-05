// Databricks notebook source
// MAGIC %run ../ADLSDBUtils

// COMMAND ----------

//Infer the schema of Investors's data
val investorSchema = StructType(List(
                                     StructField("investor_loan_id", StringType, true),
                                     StructField("loan_id", StringType, true),
                                     StructField("investor_id", StringType, false),
                                     StructField("loan_funded_amt", DoubleType, false),
                                     StructField("investor_type", StringType, false),
                                     StructField("age", IntegerType, false),
                                     StructField("state", StringType, false),
                                     StructField("country", StringType, false)
                                    
                                    )
                                )

// COMMAND ----------

// DBTITLE 1,Read the csv file into a dataframe
val investorDf = loadDataInDataframe("loan_investors",investorSchema, "raw_data", ".csv", "csv","true", ",")

// COMMAND ----------

// DBTITLE 1,Add the run date to the dataframe
//Include a run date column to signify when it got ingested into our data lake
val investorDfRunDate=addRunDate(investorDf,runDate)

// COMMAND ----------

// DBTITLE 1,Add a surrogate key to the dataframe
//Include a investor_loan_key column which acts like a surrogate key in the table
val investorData=investorDfRunDate.withColumn("investor_loan_key", sha2(concat(col("investor_loan_id"),col("loan_id"),col("investor_id")), 256))

// COMMAND ----------

// DBTITLE 1,Move the input file to archive for future use.
fileMoveToArchive("loan_investors")

// COMMAND ----------

// DBTITLE 1,Write the cleaned dataframe into data lake
//write the final cleaned customers data to data lake
//display_df.write.options(header='True').mode("append").parquet("/mnt/datasetbigdata/processed-data/lending_loan/customer_details")
