import logging
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType,FloatType



def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS real_estate_data
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    print("Keyspace created successfully")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS real_estate_data.properties (
            ward TEXT,
            price float,
            area float,
            address TEXT,
            category TEXT,
            bed TEXT,
            bath TEXT,
            link TEXT PRIMARY KEY,
            src_pic LIST<TEXT>
        )
    """)
    print("Table created successfully")

def cassandra_session():
    cluster = Cluster(["localhost"])
    session = cluster.connect()
    
    # Create keyspace and table
    create_keyspace(session)
    create_table(session)

    return session

def insert_data(session, **kwargs):
    print("Inserting Data")
    session.execute("""
        INSERT INTO real_estate_data.properties (
            ward,price, area, address, category, bed, bath, link, src_pic
        ) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(kwargs.values()))
    print("Data insert successful")

def main():
    logging.basicConfig(level=logging.INFO)

    spark = (SparkSession.builder.appName("SparkConsumer")
             .config("spark.cassandra.connection.host", "localhost")
             .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
             .getOrCreate()
             )
    
    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "real-estate-data")
                .option("startingOffsets", "earliest")
                .load())

    schema = StructType([
        StructField("ward", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("area", FloatType(), True),
        StructField("address", StringType(), True),
        StructField("category", StringType(), True),
        StructField("bed", StringType(), True),
        StructField("bath", StringType(), True),
        StructField("link", StringType(), True),
        StructField("src_pic", ArrayType(StringType()), True),
    ])

    kafka_df = (kafka_df.selectExpr("CAST(value AS STRING) as value")
                 .select(from_json(col("value"), schema).alias("data"))
                 .select("data.*"))

    def process_batch(batch_df, batch_id):
        session = cassandra_session()
        for row in batch_df.rdd.collect():
            insert_data(session, **row.asDict())
        session.shutdown()

    query = (kafka_df.writeStream
             .foreachBatch(process_batch)
             .start()
             .awaitTermination()
             )

if __name__ == "__main__":
    main()