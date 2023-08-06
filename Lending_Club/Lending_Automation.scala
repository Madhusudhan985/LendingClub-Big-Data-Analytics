// Databricks notebook source
// MAGIC %run ./ADLSDBUtils

// COMMAND ----------

// MAGIC %run ./SQLDBUtils

// COMMAND ----------

object ProcessCSVFiles {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val tableName = "LEDNING_REQUEST_LIST"

    // Read existing run information from the database
    val previousRunsDF = fetchSQLDB(s"SELECT * FROM $tableName where run_date ='$runDate'")

    // Sample notebookMapping, replace it with your actual mapping
    val notebookMapping = Map(
      "account_details.csv" -> "DataCleaning_Accounts",
      "loan_customer_data.csv" -> "DataCleaning_Customers",
      "loan_investors.csv" -> "DataCleaning_Investors",
      "loan_details.csv" -> "DataCleaning_Loan",
      "loan_defaulters.csv" -> "DataCleaning_LoanDefaulters",
      "loan_payment.csv" -> "DataCleaning_Payments"
    )
    val currentNotebookName = dbutils.notebook.getContext().notebookPath
      .getOrElse("")
      .split("/")
      .lastOption
      .getOrElse("")

    val parentPath = dbutils.notebook.getContext().notebookPath
      .getOrElse("")
      .split("/")
      .dropRight(1)
      .mkString("/")

    val rawFolderPath = "/dbfs/mnt/lendingClub/raw/lendingloan" // Replace with the actual path to your raw folder

    val rawFolder = new File(rawFolderPath)
    val csvFiles = rawFolder.listFiles(_.getName.toLowerCase.endsWith(".csv"))

    
    if (csvFiles != null) {
      println("Processing Files Started:")
      csvFiles.foreach { csvFile =>
        val notebookName = csvFile.getName.dropRight(4) // Remove ".csv" extension to get the corresponding notebook name

        // Check if the notebook has been successfully completed before
        val previousRunStatus = getPreviousRunStatus(previousRunsDF, notebookName)

        if (previousRunStatus.isEmpty || previousRunStatus.get != "completed") {
          // Run the notebook and track its status
          val notebookPath = s"$parentPath/Data-Cleaning/${notebookMapping.getOrElse(notebookName + ".csv", "")}"
          var successFlag = false // Flag to track the success of notebook execution

          try {
            runDatabricksNotebook(notebookPath)
            // If the notebook execution didn't throw any exception, mark it as successful
            successFlag = true
          } catch {
            case ex: Exception =>
              println(s"Error executing notebook: $notebookPath. Error message: ${ex.getMessage}")
          }

          // Save the notebook run information to the database
          val currentDate = new java.sql.Date(System.currentTimeMillis())
          val status = if (successFlag) "completed" else "failed"
          val newRow = Seq((currentDate, notebookName, status))
          val newRowDF = spark.sparkContext.parallelize(newRow).toDF("run_date", "notebook_name", "status")

          // Delete previous entry if it exists
          deleteFromDatabase(s"DELETE FROM $tableName WHERE notebook_name = '$notebookName' and run_date ='$runDate'")

          // Insert the new row into the database table
          insertIntoDatabase(newRowDF, tableName)
        }
      }
    } else {
      println("Raw folder does not contain 6 CSV files.")
    }
    println("Files are Processed")
  }

  def runDatabricksNotebook(notebookPath: String): Unit = {
    val runResults = dbutils.notebook.run(notebookPath, timeoutSeconds = 600)
    // println(s"Running Databricks notebook: $notebookPath")
    println(s"$runResults successfully.")
    // Since the function returns Unit, we need to handle the success status differently
  }
}


// COMMAND ----------

ProcessCSVFiles.main(Array()) // Call the main function

// COMMAND ----------

// DBTITLE 1,Creating Signal File
createTouchFile("/dbfs/mnt/lendingClub/work/sample.txt")
