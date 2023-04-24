from kafka import KafkaConsumer
import sqlite3
from sys import argv
from json import loads
from consumer import consumer
from time import time
from sys import argv

DB_CON = None

CONSUMER = None

def init_db():
	global DB_CON
	DB_CON = sqlite3.connect("app.db")
	print(f"[LOG] Connected to database")

def cleanup():
	global DB_CON
	print("[LOG] Cleaning up...")

	DB_CON.close()
	
	print("[LOG] Goodbye!")


def get_batch_data(start, count):
	global DB_CON
	cur = DB_CON.cursor()
	q = """
		SELECT * from reddit_post where id > ? LIMIT ?;
	"""
	cur.execute(q, (start, count))
	try:
		records = cur.fetchall()
	except:
		print("[ERROR] Error fetching batch data from db")
		records = None
		
	cur.close()
	return records

def get_results():
	records = get_batch_data(0, int(argv[1]))
	
	start = time()
	# run sentiment analysis on this batch
	for i in records:
		print("[LOG] Sentiment => ", i[0], " : ", consumer.analyzer_function(i[2]))
	
	end = time()
	
	print(f"[LOG] Elapsed time : {end-start}")
	

if __name__ == "__main__":
	init_db()
	try:
		get_results()

	except KeyboardInterrupt as e:
		cleanup()
