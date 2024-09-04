# Databricks notebook source
# com.azure.cosmos.spark:azure-cosmos-spark_3-5_2-12:4.30.0

# COMMAND ----------

# MAGIC %scala
# MAGIC var dfCosmosDB = spark.read.format("cosmos.oltp")
# MAGIC             .option("spark.cosmos.accountEndpoint" , "https://cosmosdbtaller01.documents.azure.com:443/")
# MAGIC             .option("spark.cosmos.accountKey" , "fumoXewgLGBPuj90mJw4VuVr2LIVlGGfGQYmtUxKHRHPzZEgQP7BZWFj7DEamkN7zqqZZMUtJlOkACDb5UkS1Q==")
# MAGIC             .option("spark.cosmos.database" , "taller_01")
# MAGIC             .option("spark.cosmos.container" , "community")
# MAGIC             .option("spark.cosmos.read.inferSchema.enabled" , "true")
# MAGIC             .load()
# MAGIC
# MAGIC display(dfCosmosDB)
# MAGIC

# COMMAND ----------


dfCosmosDBPython = (
            spark.read.format("cosmos.oltp")
                .option("spark.cosmos.accountEndpoint" , "https://cosmosdbtaller01.documents.azure.com:443/")
                .option("spark.cosmos.accountKey" , "fumoXewgLGBPuj90mJw4VuVr2LIVlGGfGQYmtUxKHRHPzZEgQP7BZWFj7DEamkN7zqqZZMUtJlOkACDb5UkS1Q==")
                .option("spark.cosmos.database" , "taller_01")
                .option("spark.cosmos.container" , "community")
                .option("spark.cosmos.read.inferSchema.enabled" , "true")
                .load()
            )

display(dfCosmosDBPython)


# COMMAND ----------


dfCosmosStreaming = (
            spark.readStream.format("cosmos.oltp.changeFeed")
                .option("spark.cosmos.accountEndpoint" , "https://cosmosdbtaller01.documents.azure.com:443/")
                .option("spark.cosmos.accountKey" , "fumoXewgLGBPuj90mJw4VuVr2LIVlGGfGQYmtUxKHRHPzZEgQP7BZWFj7DEamkN7zqqZZMUtJlOkACDb5UkS1Q==")
                .option("spark.cosmos.database" , "taller_01")
                .option("spark.cosmos.container" , "community")
                .option("spark.cosmos.read.inferSchema.enabled" , "true")
                .load()
            )

( dfCosmosStreaming
    .writeStream
    .trigger(processingTime="2 seconds")
    .outputMode("append")
    .option("checkpointLocation", "FileStore/taller01/streaming/cosmosdb/")
    .toTable("taller_comunity01")
)

