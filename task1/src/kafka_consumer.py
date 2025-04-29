import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("TikTok Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .getOrCreate()

def process_stream():
    spark = create_spark_session()
    
    video_schema = StructType([
        StructField("type", StringType(), True),
        StructField("data", StructType([
            StructField("id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("caption", StringType(), True),
            StructField("create_time", TimestampType(), True),
            StructField("like_count", IntegerType(), True),
            StructField("comment_count", IntegerType(), True),
            StructField("view_count", IntegerType(), True),
            StructField("share_count", IntegerType(), True),
            StructField("collected_at", TimestampType(), True)
        ]), True)
    ])
    
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS")) \
        .option("subscribe", os.getenv("KAFKA_TOPIC")) \
        .option("startingOffsets", "latest") \
        .load()
    
    value_df = kafka_df.select(
        from_json(col("value").cast("string"), video_schema).alias("parsed_value")
    )
    
    video_df = value_df.filter(col("parsed_value.type") == "video_data") \
        .select("parsed_value.data.*")
    
    engagement_df = video_df.withColumn(
        "engagement_score", 
        (col("like_count") * 2 + col("comment_count") * 3 + col("share_count") * 5) / col("view_count")
    )
    
    query = engagement_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    """
    query = engagement_df.writeStream \
        .foreachBatch(lambda df, epoch_id: df.write \
            .jdbc(
                url=f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
                table="video_metrics_realtime",
                mode="append",
                properties={
                    "user": os.getenv("DB_USER"),
                    "password": os.getenv("DB_PASSWORD"),
                    "driver": "org.postgresql.Driver"
                }
            )
        ) \
        .start()
    """
    query.awaitTermination()

def main():
    try:
        logger.info("Starting Kafka consumer")
        process_stream()
    except Exception as e:
        logger.error(f"Error in Kafka consumer: {e}")

if __name__ == "__main__":
    main() 