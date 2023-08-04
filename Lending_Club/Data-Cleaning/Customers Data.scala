// Databricks notebook source
import org.apache.spark.sql.types.{StructType,StructField,IntegerType,StringType,TimestampType}
import org.apache.spark.sql.functions.{col,concat,current_timestamp,sha2}

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
