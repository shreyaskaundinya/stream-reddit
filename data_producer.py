from configparser import ConfigParser

import praw
from time import sleep  
from json import dumps  
from kafka import KafkaProducer  
import json
from sys import argv
import threading

configParser = ConfigParser()
config = configParser.read("config")
CLIENT_ID = configParser["DATA_PRODUCER"].get("client_id")
CLIENT_SECRET = configParser["DATA_PRODUCER"].get("client_secret")
USER_AGENT = configParser["DATA_PRODUCER"].get("user_agent")

#print(CLIENT_ID, CLIENT_SECRET, USER_AGENT)

reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=USER_AGENT)
HOT_POSTS = reddit.subreddit(argv[1]).hot()


#for x in HOT_POSTS:
#    print(x.selftext)
#print(type(HOT_POSTS.hot()))
#print(dir(praw.models.reddit.subreddit.Subreddit))



# Kafka broker information
kafka_broker = 'localhost:9092'
kafka_topic = argv[1]
interval = 5

# print(dir(HOT_POSTS))
l = []
dict1= {}
for x in HOT_POSTS:
    try:
        #print(x.title)
        l.append(
		    {
				"id": str(x.id),
				"selftext": str(x.selftext),
				"title": str(x.title),
				"created": str(x.created_utc)
		   	}
       	)
    except:
        #print(e)
        pass

my_producer = KafkaProducer(bootstrap_servers = kafka_broker)

def myPeriodicFunction(l, start):
	global kafka_topic
	print(f"[LOG] SENDING DATA to BROKER from subreddit= r/{kafka_topic}!")
	for i in range(start, start+5):
		my_producer.send(kafka_topic, json.dumps(l[i]).encode('utf-8'))
	
	my_producer.flush()


count = 0

def startTimer():
        global count
        threading.Timer(interval, startTimer).start()
        #print("HELLOOOOO", count)
        myPeriodicFunction(l, count)
        if(count<100):
            count = count + 5
        else:
            return

startTimer()


'''
['STR_FIELD', '__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattr__', 
'__getattribute__', '__gt__', '__hash__', '__init__', '__init_subclass__', '__le__', '__lt__', '__module__', '__ne__', '__new__', 
'__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_chunk', 
'_comments_by_id', '_fetch', '_fetch_data', '_fetch_info', '_fetched', '_kind', '_reddit', '_reset_attributes', '_safely_add_arguments', 
'_url_parts', '_vote', 'all_awardings', 'allow_live_comments', 'approved_at_utc', 'approved_by', 'archived', 'author', 
'author_flair_background_color', 'author_flair_css_class', 'author_flair_richtext', 'author_flair_template_id', 'author_flair_text', 
'author_flair_text_color', 'author_flair_type', 'author_fullname', 'author_is_blocked', 'author_patreon_flair', 'author_premium', 'award', 
'awarders', 'banned_at_utc', 'banned_by', 'can_gild', 'can_mod_post', 'category', 'clear_vote', 'clicked', 'comment_limit', 'comment_sort', 
'comments', 'content_categories', 'contest_mode', 'created', 'created_utc', 'crosspost', 'delete', 'disable_inbox_replies', 'discussion_type', 
'distinguished', 'domain', 'downs', 'downvote', 'duplicates', 'edit', 'edited', 'enable_inbox_replies', 'flair', 'fullname', 'gild', 'gilded', 
'gildings', 'hidden', 'hide', 'hide_score', 'id', 'id_from_url', 'is_created_from_ads_ui', 'is_crosspostable', 'is_meta', 'is_original_content', 
'is_reddit_media_domain', 'is_robot_indexable', 'is_self', 'is_video', 'likes', 'link_flair_background_color', 'link_flair_css_class', 
'link_flair_richtext', 'link_flair_text', 'link_flair_text_color', 'link_flair_type', 'locked', 'mark_visited', 'media', 'media_embed', 
'media_only', 'mod', 'mod_note', 'mod_reason_by', 'mod_reason_title', 'mod_reports', 'name', 'no_follow', 'num_comments', 'num_crossposts', 
'num_reports', 'over_18', 'parent_whitelist_status', 'parse', 'permalink', 'pinned', 'pwls', 'quarantine', 'removal_reason', 'removed_by', 
'removed_by_category', 'reply', 'report', 'report_reasons', 'save', 'saved', 'score', 'secure_media', 'secure_media_embed', 'selftext', 
'selftext_html', 'send_replies', 'shortlink', 'spoiler', 'stickied', 'subreddit', 'subreddit_id', 'subreddit_name_prefixed', 
'subreddit_subscribers', 'subreddit_type', 'suggested_sort', 'thumbnail', 'thumbnail_height', 'thumbnail_width', 'title', 'top_awarded_type', 
'total_awards_received', 'treatment_tags', 'unhide', 'unsave', 'ups', 'upvote', 'upvote_ratio', 'url', 'user_reports', 'view_count', 'visited', 
'whitelist_status', 'wls']
'''
