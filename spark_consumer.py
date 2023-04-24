from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.streaming import StreamingContext
from kafka import KafkaConsumer
# from pyspark.streaming.Kafka import KafkaUtils
from pyspark.sql.functions import decode
from pyspark.sql.types import StringType, DecimalType
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import time
import json

kafka_broker = 'localhost:9092'
kafka_topic = 'reddit_dbt'
spark = SparkSession.builder.appName("DataConsumer").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()

#kafka_consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_broker)


"""

for message in kafka_consumer:
    reddit_data = message.value.decode('utf-8')
    print(reddit_data)
"""


analyzer = SentimentIntensityAnalyzer()

def analyzer_function(post):
	#print(post)
	sentiment = analyzer.polarity_scores(post)
	if(sentiment["compound"]>0.05):
		return "good"
	elif(sentiment["compound"]<0.05):
		return "bad"
	else:
		return "neutral"
	
def give_me_selftext(post):
	try:
		st = json.loads(post)["selftext"]
		if (st is not None):
			return st
		else:
			return " "
	except:
		return " "
	
udf_json = udf(lambda x: give_me_selftext(x), StringType())
udf_analyzer_function = udf(analyzer_function, StringType())

def read_from_stream():
	df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_broker).option("subscribe", kafka_topic).load()
	df = df.withColumn('ACTUAL_VALUE',decode(df.value, "UTF-8").cast("string"))
	# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

	df_val = df.withColumn("selftext", udf_json(df.ACTUAL_VALUE))
	df_res = df_val.withColumn("res", udf_analyzer_function(df_val.selftext))
	
	# senti_val.show()
	
	df_res.select("selftext", "res").writeStream.format("console").start().awaitTermination()
	# df_val.select("selftext").writeStream.format("console").start().awaitTermination()


read_from_stream()
