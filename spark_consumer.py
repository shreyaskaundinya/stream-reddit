from pyspark.sql import SparkSession, DataFrameWriterV2 
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.streaming import StreamingContext
from kafka import KafkaConsumer
# from pyspark.streaming.Kafka import KafkaUtils
from pyspark.sql.functions import decode
from pyspark.sql.types import StringType, DecimalType, IntegerType
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from consumer import analyzer_function
from sys import argv
import tempfile

import time
import json

kafka_broker = 'localhost:9092'
kafka_topic = argv[1]
spark = SparkSession.builder.appName("DataConsumer").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()

#kafka_consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_broker)


"""

for message in kafka_consumer:
    reddit_data = message.value.decode('utf-8')
    print(reddit_data)
"""
	
def give_me_selftext(post):
	try:
		st = json.loads(post)["selftext"]
		if (st is not None):
			return st
		else:
			return " "
	except:
		return " "
	
def naive_wc(selftext):
	return len(selftext.split(" "))

udf_json = udf(lambda x: give_me_selftext(x), StringType())
udf_analyzer_function = udf(analyzer_function, StringType())
udf_naive_wc = udf(naive_wc, IntegerType())

def read_from_stream():
	df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_broker).option("subscribe", kafka_topic).load()
	df = df.withColumn('ACTUAL_VALUE',decode(df.value, "UTF-8").cast("string"))

	df_val = df.withColumn("selftext", udf_json(df.ACTUAL_VALUE))
	start = time.time()
	df_sent = df_val.withColumn("_sentiment", udf_analyzer_function(df_val.selftext))
	end = time.time()
	
	df_eps = spark.createDataFrame([(f"{end-start}",)], schema=["Elapsed time!!!!",])

	
	df_wc = df_sent.withColumn("_wc", udf_naive_wc(df_val.selftext))	

	df_wc.select("selftext", "_sentiment", "_wc").writeStream.format("console").start().awaitTermination()



if __name__ == "__main__":
	read_from_stream()

