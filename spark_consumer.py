from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.streaming import StreamingContext
from kafka import KafkaConsumer

#spark = SparkSession.builder.appName("DataConsumer").getOrCreate()
kafka_consumer = KafkaConsumer("reddit_data", bootstrap_servers='localhost:9092')

for message in kafka_consumer:
    reddit_data = message.value.decode('utf-8')
    print(reddit_data)
