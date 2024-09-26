# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": '99e2eb97-be6f-48e1-a2e4-3c9b5b6b4bce',
          "fs.azure.account.oauth2.client.secret": 'CDh8Q~QL~1ksNvQtBntjrNohenmu98yGCHpIabGR',
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/28b966a6-604f-47b9-894a-e27d486378c6/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://paris-olympics-data@synapsewsadl.dfs.core.windows.net/",
  mount_point = "/mnt/synapsewsadl/paris-olympics-data",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/synapsewsadl/paris-olympics-data"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/synapsewsadl/paris-olympics-data

# COMMAND ----------

athletes_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/mnt/synapsewsadl/paris-olympics-data/raw-data/athletes.csv")
coaches_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/mnt/synapsewsadl/paris-olympics-data/raw-data/coaches.csv")
entriesgender_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/mnt/synapsewsadl/paris-olympics-data/raw-data/entriesgender.csv")
medals_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/mnt/synapsewsadl/paris-olympics-data/raw-data/medals.csv")
teams_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/mnt/synapsewsadl/paris-olympics-data/raw-data/teams.csv")

# COMMAND ----------

spark

# COMMAND ----------

athletes_df.show()

# COMMAND ----------

coaches_df.show()

# COMMAND ----------

entriesgender_df.show()

# COMMAND ----------

medals_df.show()

# COMMAND ----------

teams_df.show()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

entriesgender_schema = StructType(fields = [StructField ("Discipline" ,StringType(), True),
                                            StructField ("Female" ,IntegerType(), True),
                                            StructField ("Male" ,IntegerType(), True),
                                            StructField ("Total" ,IntegerType(), True)])

# COMMAND ----------

medals_schema = StructType(fields=[StructField("Rank", IntegerType(), False),
                                   StructField("TeamCountry", StringType(), True),
                                   StructField("Gold", IntegerType(), True),
                                   StructField("Silver", IntegerType(), True),
                                   StructField("Bronze", IntegerType(), True),
                                   StructField("Total", IntegerType(), True),
                                   StructField("Rank by Total", StringType(), False)])

# COMMAND ----------

entriesgender_df = entriesgender_df.withColumn("Female", col("Female").cast(IntegerType()))\
                                   .withColumn("Male", col("Male").cast(IntegerType()))\
                                    .withColumn("Total", col("Total").cast(IntegerType()))

# COMMAND ----------

entriesgender_df.printSchema()

# COMMAND ----------

medals_df.printSchema()

# COMMAND ----------

# Find the top countries with the highest number of Gold medals in the Olympics

top_gold_medals_countries = medals_df.orderBy("Gold", ascending=False).select("TeamCountry", "Gold").show()

# COMMAND ----------

# Calculate the average numbers of genders for each discipline

average_entries_by_gender = entriesgender_df.withColumn(
    'Avg_Female', entriesgender_df['Female'] / entriesgender_df['Total']
).withColumn(
    'Avg_Male', entriesgender_df['Male'] / entriesgender_df['Total']
)
average_entries_by_gender.show()

# COMMAND ----------

athletes_df.repartition(3).write.mode("overwrite").parquet("/mnt/synapsewsadl/paris-olympics-data/transformed-data/athletes")

# COMMAND ----------

coaches_df.repartition(1).write.mode("overwrite").parquet("/mnt/synapsewsadl/paris-olympics-data/transformed-data/coaches")

# COMMAND ----------

entriesgender_df.repartition(1).write.mode("overwrite").parquet("/mnt/synapsewsadl/paris-olympics-data/transformed-data/entriesgender")

# COMMAND ----------

medals_df.repartition(1).write.mode("overwrite").parquet("/mnt/synapsewsadl/paris-olympics-data/transformed-data/medals")

# COMMAND ----------

teams_df.repartition(1).write.mode("overwrite").parquet("/mnt/synapsewsadl/paris-olympics-data/transformed-data/teams")
