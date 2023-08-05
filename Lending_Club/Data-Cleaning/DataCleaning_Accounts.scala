// Databricks notebook source
// MAGIC %run ../ADLSDBUtils

// COMMAND ----------

//Infer the schema of the accounts's data
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

// DBTITLE 1,Read the csv file into a dataframe
val accountDf = loadDataInDataframe("account_details",accountSchema, "raw_data", ".csv", "csv", "true", ",")

// COMMAND ----------

display(accountDf)

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Optimized way to clean your dataframe having extra strings and symbols from a dataframe column

// COMMAND ----------

val cleanAccountDf = accountDf
  .withColumn("emp_length", regexp_replace(col("emp_length"), "[^0-9]", " "))
  .withColumn("emp_length", col("emp_length").cast("integer"))
  .withColumn("emp_length", when(col("emp_length") === "null", lit(null)).otherwise(col("emp_length"))) //Replace the NULL strings into NULL values


// COMMAND ----------

    // List of columns to process
    val columnsToProcess = Seq("col1", "col2", "col3")

    // Replace "null" strings with null values and cast to FloatType
    val updatedAccountDf = columnsToProcess.foldLeft(accountDf) { (accDf, colName) =>
      accDf.withColumn(colName, when(col(colName) === "null", lit(null)).otherwise(col(colName)))
        .withColumn(colName, col(colName).cast("float"))
    }

    // Show the updated DataFrame
    updatedAccountDf.show()

    // Stop the Spark session
    spark.stop()
  }
}


// COMMAND ----------

display(cleanAccountDf)

// COMMAND ----------

cleanAccountDf.createOrReplaceTempView("temp")
val display_df=spark.sql("select * from temp where tot_hi_cred_lim is null ")
display(display_df)

// COMMAND ----------

// DBTITLE 1,Rename the columns to a better understandable way
val renamedCustomerDf=cleanAccountDf.withColumnRenamed("cust_id","customer_id")
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
