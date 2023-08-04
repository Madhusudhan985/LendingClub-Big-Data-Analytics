// Databricks notebook source
val adlsRawFilePath = "/mnt/lendingClub/"

// COMMAND ----------

// DBTITLE 1,Mounting
val configs =  Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id"  ->s"61438f0b-28e0-48bc-92ab-698cb40e129a",
  "fs.azure.account.oauth2.client.secret" ->s"bED8Q~OB8gldQGcYdkhFyqoUacgKULoCopzZNc7D",
  "fs.azure.account.oauth2.client.endpoint" ->s"https://login.microsoftonline.com/7299dff3-9f02-46f8-bd12-5ec6d1fb678c/oauth2/token"
)


// COMMAND ----------

//Mounting ADLS Storage to DBFS
dbutils.fs.mount(
  source = "abfss://lendingclub@lendingstoragesumit.dfs.core.windows.net/",
  mountPoint = "/mnt/lendingClub",
  extraConfigs = configs
)

// COMMAND ----------

def loadDataInDataframe(tableName:String,schema:StructType,inBoundSource:String,tableNameSuffix:String,format:String,header:String,delimiter:String): DataFrame=
{
  sqlContext.read.format(format)
  .option("header",header)
  .option("delimiter",delimiter)
  .option("nullValue",null)
  .option("nullValue","")
  .option("quote","")
  .option("mode","FAILFAST")
  .schema(schema)
  .load(adlsRawFilePath+inBoundSource+"/"+tableName+tableNameSuffix).drop("ExtraColumn")
}

// COMMAND ----------

def writePartitionDataInDeltaLakeAndCreateTable(tableName:String,tableSchema:String,data:DataFrame,deltaPath:String,partitionColName:String)={
  println("Writing table:"+tableName)
  
}
