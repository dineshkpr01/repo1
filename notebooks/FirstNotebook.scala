// Databricks notebook source
import java.util.Properties

// COMMAND ----------

val co_jdbcUsername = "db_user"
val co_jdbcPassword = "SuperSecret!"
val co_jdbcHostname = "lazuresqlserver.database.windows.net"
val co_jdbcPort = 1433
val co_jdbcDatabase = "azuresqldb01"

// Create the JDBC URL without passing in the user and password parameters.
val co_jdbcUrl = s"jdbc:sqlserver://${co_jdbcHostname}:${co_jdbcPort};database=${co_jdbcDatabase}"

// Create a Properties() object to hold the parameters.
val co_connectionProperties = new Properties()

co_connectionProperties.put("user", s"${co_jdbcUsername}")
co_connectionProperties.put("password", s"${co_jdbcPassword}")

val co_driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
co_connectionProperties.setProperty("Driver", co_driverClass)

// COMMAND ----------

val df_sample_table = spark.read.jdbc(co_jdbcUrl, s"dbo.Sample", co_connectionProperties)

// COMMAND ----------

display(df_sample_table)

// COMMAND ----------

val df_newRow = spark.sql(s"SELECT 2 AS id, 'dddd' AS name, 1 as isActive")
display(df_newRow)
df_newRow.write.mode(SaveMode.Append).jdbc(co_jdbcUrl,s"dbo.Sample",  co_connectionProperties)

