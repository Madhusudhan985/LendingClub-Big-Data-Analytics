// Databricks notebook source
import java.io.File
import java.time.LocalDate
import java.util.Date
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col,concat,current_timestamp,sha2,regexp_replace,lit,to_date}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, DataFrame}

// COMMAND ----------

val container = "/mnt/lendingClub/"
val runDate = java.time.LocalDate.now() // Call the function to get the current date
val sourceLocation = container + "raw/lendingloan/"
val destLocation = "/dbfs" + sourceLocation + "archive/run_date=" + runDate
val rqstTemplateExtension = "csv"

// COMMAND ----------

def loadDataInDataframe(tableName:String,schema:StructType,inBoundSource:String,tableNameSuffix:String,format:String,header:String,delimiter:String): DataFrame=
{
  sqlContext.read.format(format)
  .option("header",header)
  .option("delimiter",delimiter)
  .option("nullValue",null)
  .option("nullValue","")
  .option("quote","\"")
  .schema(schema)
  .load(container+inBoundSource+"/"+tableName+tableNameSuffix).drop("ExtraColumn")
}

// COMMAND ----------

def writePartitionDataInDeltaLakeAndCreateTable(tableName:String,tableSchema:String,data:DataFrame,deltaPath:String,partitionColName:String)={
  println("Writing table:"+tableName)
  data.write
  .format("delta")
  .mode("overwrite")
  .partitionBy(partitionColName)
  .save(container+deltaPath+"/"+tableName+"/")

  spark.sql("CREATE TABLE IF NOT EXISTS "+tableSchema+"."+tableName+" USING DELTA LOCATION '"+container+deltaPath+"/"+tableName+"/' ");
  
}

// COMMAND ----------

def writePartitionDataInParquetAndCreateTable(tableName: String, tableSchema: String, data: DataFrame, parquetPath: String, partitionColName: String) = {
  println("Writing table:" + tableName)
  data.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy(partitionColName)
    .save(container + parquetPath + "/" + tableName + "/")

  spark.sql("CREATE TABLE IF NOT EXISTS " + tableSchema + "." + tableName + " USING PARQUET OPTIONS (PATH '" + container + parquetPath + "/" + tableName + "/')")
}


// COMMAND ----------

def writePartitionDataInParquet(tableName: String,data: DataFrame, parquetPath: String, partitionColName: String) = {
  println("Writing table:" + tableName)
  data.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy(partitionColName)
    .save(container + parquetPath + "/" + tableName + "/")
}

// COMMAND ----------

def fileMoveToArchive(rqstFileName: String): Unit = {
  val destDir = new File(destLocation)
  if (!destDir.isDirectory) {
    destDir.mkdir()
  }
  val sourceFile = new File("/dbfs" + sourceLocation + rqstFileName + "." + rqstTemplateExtension)
  val destinationFile = new File(destLocation + "/" + rqstFileName + "." + rqstTemplateExtension)
  
  try {
    if (sourceFile.renameTo(destinationFile)) {
      println("File Moved Successfully.")
    } else {
      println("File Move Failed.")
    }
  } catch {
    case e: Exception => println("An error occurred: " + e.getMessage)
  }
}


// COMMAND ----------

def addRunDate(input_df: DataFrame, runDate: LocalDate): DataFrame = {
    val date_df = input_df.withColumn("run_date", lit(runDate))
    date_df
}
