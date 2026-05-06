# Configuration
storage_account_name = "finstreamdatalake"
storage_account_key = dbutils.secrets.get(
    scope="finstream-scope", 
    key="storage-account-key"
)
eventhub_connection_string = dbutils.secrets.get(
    scope="finstream-scope", 
    key="eventhub-connection-string"
)

# Mount ADLS Gen2
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("Configuration set successfully")


# Define storage paths
bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/transactions"
checkpoint_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/checkpoints/transactions"

print(f"Bronze path: {bronze_path}")
print(f"Checkpoint path: {checkpoint_path}")

# Event Hubs connection configuration
# Databricks connects to Event Hubs using the Kafka protocol
ehConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        eventhub_connection_string
    ),
    "eventhubs.consumerGroup": "$Default",
    "eventhubs.startingPosition": '{"offset": "-1", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}'
}

print("Event Hubs configuration ready")

# Read streaming data from Event Hubs
raw_stream = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

print("Stream reader created")
print("Schema:")
raw_stream.printSchema()

from pyspark.sql.functions import col, current_timestamp, input_file_name
from pyspark.sql.types import StringType

# The body comes in as binary — convert to string
# Add metadata columns for observability
bronze_stream = raw_stream.select(
    col("body").cast(StringType()).alias("raw_payload"),
    col("partition").alias("source_partition"),
    col("offset").alias("source_offset"),
    col("sequenceNumber").alias("sequence_number"),
    col("enqueuedTime").alias("enqueued_time"),
    current_timestamp().alias("ingestion_timestamp")
)

# Write to Bronze Delta table — append only, never update
query = bronze_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .option("mergeSchema", "true") \
    .start(bronze_path)

print("Bronze streaming pipeline started!")
print(f"Writing to: {bronze_path}")

# Check how many records landed in Bronze
bronze_df = spark.read.format("delta").load(bronze_path)
print(f"Total records in Bronze: {bronze_df.count()}")
bronze_df.show(5, truncate=False)