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

// MAGIC %md #####Read the csv file into a dataframe

// COMMAND ----------

val investorDf = loadDataInDataframe("loan_investors",investorSchema, "raw", ".csv", "csv","true", ",")

// COMMAND ----------

// MAGIC %md #####Add the run date to the dataframe

// COMMAND ----------

//Include a run date column to signify when it got ingested into our data lake
val investorDfRunDate=addRunDate(investorDf,runDate)

// COMMAND ----------

// MAGIC %md #####Add a surrogate key to the dataframe

// COMMAND ----------

//Include a investor_loan_key column which acts like a surrogate key in the table
val investorData=investorDfRunDate.withColumn("investor_loan_key", sha2(concat(col("investor_loan_id"),col("loan_id"),col("investor_id")), 256))

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Use Spark SQL to query the data

// COMMAND ----------

//Using spark SQL to query the tables
investorData.createOrReplaceTempView("investor_temp_table")
val finalInvestorDf=spark.sql("""select investor_loan_key,run_date,investor_loan_id,loan_id,investor_id,loan_funded_amt,investor_type,age,state,country from investor_temp_table""")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Write the cleaned dataframe into data lake

// COMMAND ----------

writePartitionDataInParquetAndCreateTable("investors","work",finalInvestorDf,"work","run_date")


// COMMAND ----------

// MAGIC %md #####Move the input file to archive for future use.

// COMMAND ----------

fileMoveToArchive("loan_investors")

// COMMAND ----------

dbutils.notebook.exit("executed investors job")
