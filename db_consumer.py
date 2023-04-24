from kafka import KafkaConsumer
import sqlite3
from sys import argv
from json import loads

DB_CON = None

CONSUMER = None
TOPIC = argv[1]

def init_db():
	global DB_CON
	DB_CON = sqlite3.connect("app.db")
	print(f"[LOG] Connected to database")

def init_consumer():
	global CONSUMER
	print(f"[LOG] Creating database consumer!")
	CONSUMER = KafkaConsumer(bootstrap_servers="localhost:9092")
	print(f"[LOG] Subscribing to {TOPIC}")
	CONSUMER.subscribe([TOPIC])

def cleanup():
	global DB_CON, CONSUMER
	print("[LOG] Cleaning up...")
	
	CONSUMER.close()
	DB_CON.close()
	
	print("[LOG] Goodbye!")

def insert_post(post, topic):
	"""
	Post : {id, title, selftext, created}
	"""
	global DB_CON
	cur = DB_CON.cursor()
	
	q = """
		INSERT into reddit_post values (?, ?, ?, ?, ?);
	"""
	try:
		cur.execute(q, (post["id"], post["title"], post["selftext"], topic, post["created"]))
		DB_CON.commit()
		print(f"[LOG] Added post with id={post['id']} to db")
	except Exception as err:
		print(f"[ERROR] Error inserting post with id={post['id']} with error={err}")
		 
	cur.close()
	

def create_table():
	global DB_CON
	cur = DB_CON.cursor()
	q = """
		CREATE table if not exists reddit_post (
			id int primary key,
			title varchar(256),
			selftext varchar(2000),	
			subreddit varchar(200),
			created varchar(100)
		);
	"""
	cur.execute(q)
	print(f"[LOG] Creating/Using table reddit_post")
	DB_CON.commit()
	cur.close()


if __name__ == "__main__":
	init_db()
	create_table()
	init_consumer()
	try:
		for msg in CONSUMER:
			insert_post(loads(msg.value.decode("utf-8")), TOPIC)
			# print(f"[LOG] Received {msg.value.decode('utf-8')}")

	except KeyboardInterrupt as e:
		cleanup()
