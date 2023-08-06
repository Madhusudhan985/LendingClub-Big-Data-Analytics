// Databricks notebook source
import org.apache.spark.sql.{DataFrame, SparkSession}

// COMMAND ----------

import java.util.Properties
import java.sql.{Connection,DriverManager}
import java.util
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.query._
import com.microsoft.azure.sqldb.spark.connect._


// COMMAND ----------

val sqlUserName = "madhu"
val sqlPassword = "Pavankalyan@21"


// COMMAND ----------

val jdbcUrl = "jdbc:sqlserver://lendingclub.database.windows.net:1433;database=lendingDB;user=madhu@lendingclub;password=Pavankalyan@21;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

val connectionProperties = new Properties()

connectionProperties.put("user",sqlUserName)
connectionProperties.put("password",sqlPassword)

// COMMAND ----------

def insertIntoDatabase(dataframe: DataFrame,tableName: String): Unit = {
      dataframe.write.mode("Append").jdbc(jdbcUrl, tableName, connectionProperties)
  }

// COMMAND ----------

def fetchSQLDB(query : String):DataFrame =
{
  var collection = spark.read.format("jdbc")
                        .option("url",jdbcUrl)
                        .option("query",query)
                        .option("user",sqlUserName)
                        .option("password",sqlPassword)
                        .load()
  return collection
}

// COMMAND ----------

 def deleteFromDatabase(query: String): Unit = {
    val connection = DriverManager.getConnection(jdbcUrl,sqlUserName,sqlPassword)
    connection.setAutoCommit(true)
    val statement = connection.createStatement()
    statement.execute(query)
  }

// COMMAND ----------

import org.apache.spark.sql.{DataFrame, Row}

def getPreviousRunStatus(previousRunsDF: DataFrame, notebookName: String): Option[String] = {
  // Filter the DataFrame to get the previous run status for the given notebook name
  val filteredDF = previousRunsDF.filter($"notebook_name" === notebookName)

  // Get the first row of the filtered DataFrame (if it exists)
  val firstRow: Option[Row] = filteredDF.take(1).headOption

  // Extract the "status" value from the first row if it exists
  val firstStatus: Option[String] = firstRow.map(row => row.getAs[String]("status"))

  // Return the status
  firstStatus
}

