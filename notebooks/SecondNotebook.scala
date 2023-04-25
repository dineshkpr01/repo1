// Databricks notebook source
// DBTITLE 1,Mount Storage Account file
val containerName = "container01"
val storageAccountName = "azurestatore01"
val sas = "?sv=2021-12-02&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2023-05-05T19:50:36Z&st=2023-04-25T11:50:36Z&spr=https,http&sig=edVACj0gvKCW%2FRSazMSjpyQ%2Bawmc5fknhIEELkiqzBQ%3D"
val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"
 
dbutils.fs.mount(
  source = "wasbs://"+containerName+"@"+storageAccountName+".blob.core.windows.net/folder01/sample.csv",
  mountPoint = "/mnt/myfile",
  extraConfigs = Map(config -> sas)
)
 
 
val mydf = spark.read.option("header","true").option("inferSchema", "true").csv("/mnt/myfile")
display(mydf)

// COMMAND ----------

// DBTITLE 1,Read Data from Storage Account

val adls_storage_account_name = "azurestatore01"
val adls_container_name = "container01"
val folder_path = "folder01"
val file_name = "sample.csv"
val file_path = s"abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+folder_path+"/"+file_name;


val storage_account_access_key = "suBdlcoXNQWOucOV2mnfUs8csS3aFE4AcH/Q+MF0xIhPQUUKIWy6Uq07nXcttqJN6xCoxihnq/pE+AStQlcV4A=="

spark.conf.set("fs.azure.account.key."+adls_storage_account_name+".blob.core.windows.net", storage_account_access_key)

val df = spark.read.option("inferSchema", "true").load(file_path)

display(df)
