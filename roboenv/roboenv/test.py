from pyspark.sql import SparkSession

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/robo_advisor?retryWrites=true&w=majority"
database_name = "robo_advisor"
collection_name = "enhanced_asset_data"

# Set up Spark session
spark = SparkSession.builder \
    .appName("TestMongoDBConnection") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .config("spark.mongodb.input.uri", mongo_uri) \
    .getOrCreate()

try:
    # Fetch data from MongoDB
    df = spark.read.format("mongodb") \
        .option("uri", mongo_uri) \
        .option("database", database_name) \
        .option("collection", collection_name) \
        .load()

    if df.head(1):
        print("Connection successful! Sample data:")
        df.show(5)
    else:
        print("Connection successful, but the collection is empty.")
except Exception as e:
    print(f"Failed to connect to MongoDB: {e}")
finally:
    # Stop the Spark session
    spark.stop()
