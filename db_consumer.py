from kafka import KafkaConsumer
import sqlite3
from sys import argv
from json import loads

DB_CON = None

CONSUMER = None

def init_db():
	global DB_CON
	DB_CON = sqlite3.connect("app.db")
	print(f"[LOG] Connected to database")

def init_consumer():
	global CONSUMER
	print(f"[LOG] Creating database consumer!")
	CONSUMER = KafkaConsumer(bootstrap_servers="localhost:9092")
	print(f"[LOG] Subscribing to {argv[1]}")
	CONSUMER.subscribe([argv[1]])

def cleanup():
	global DB_CON, CONSUMER
	print("[LOG] Cleaning up...")
	CONSUMER.close()
	DB_CON.close()
	print("[LOG] Goodbye!")

def insert_post(post):
	"""
	Post : {id, title, selftext, created}
	"""
	print(type(post), post)
	global DB_CON
	cur = DB_CON.cursor()
	q = """
		INSERT into reddit_post values (?, ?, ?, ?);
	"""
	cur.execute(q, post["id"], post["title"], post["selftext"], post["created"])
	DB_CON.commit()
	cur.close()
	#print(f"[LOG] Added post with id={post["id"]} to db")

def create_table():
	global DB_CON
	cur = DB_CON.cursor()
	q = """
		CREATE table if not exists reddit_post (
			id int primary key,
			title varchar(256),
			selftext varchar(2000),
			created varchar(100)
		);
	"""
	cur.execute(q)
	print(f"[LOG] Creating/Using table reddit_post")
	DB_CON.commit()
	cur.close()

def get_batch_data(start, count):
	global DB_CON
	cur = DB_CON.cursor()
	q = """
		SELECT * from reddit_post where id > ? LIMIT ?;
	"""
	cur.execute(q, start, count)
	try:
		records = cur.fetchall()
	except:
		print("[ERROR] Error fetching batch data from db")
		records = None
		
	cur.close()
	return records
	

if __name__ == "__main__":
	init_db()
	create_table()
	init_consumer()
	try:
		for msg in CONSUMER:
			insert_post(loads(msg.value.decode("utf-8")))
			print(f"[LOG] Received {msg.value.decode('utf-8')}")

	except KeyboardInterrupt as e:
		cleanup()
