from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.streaming import StreamingContext
from kafka import KafkaConsumer
kafka_broker = 'localhost:9092'
kafka_topic = 'reddit_dbt'
#spark = SparkSession.builder.appName("DataConsumer").getOrCreate()
kafka_consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_broker)

for message in kafka_consumer:
    reddit_data = message.value.decode('utf-8')
    print(reddit_data)
