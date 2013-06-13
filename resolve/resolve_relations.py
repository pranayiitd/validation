from py2neo import neo4j
import json
from pprint import pprint
from datetime import datetime
import time
import os
import oauth2 as oauth
import twitter
import os
import sys

def dump_log(loc, arr):
	f = open(loc,"a")
	s =""	
	for elem in arr:
		s = s+str(elem)+"\t"
	s = s+"\n"
	f.write(s)
	f.close()

def setup_config(loc):
	"""
	Setup the API config using the credentials in loc.
	"""
	f = open(loc, "r")
	lines = f.readlines()
	i = 0
	api_config = []
	while (i + 3) < len(lines) :
		api_id = {}
		api_id['c_key'] = lines[i].replace("\n", "")
		api_id['c_sec'] = lines[i + 1].replace("\n", "")
		api_id['a_key'] = lines[i + 2].replace("\n", "")
		api_id['a_sec'] = lines[i + 3].replace("\n", "")
		api_config.append(api_id)
		i += 5

	return api_config

def setup_client(api_id):
	"""
	Return api_client to communicate with Twitter.
	"""
	app = api_id
	CONSUMER_KEY = app['c_key']
	CONSUMER_SECRET = app['c_sec']
	ACCESS_KEY = app['a_key']
	ACCESS_SECRET = app['a_sec']
	consumer = oauth.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
	access_token = oauth.Token(key=ACCESS_KEY, secret=ACCESS_SECRET)
	client = oauth.Client(consumer, access_token)
	return client	

def insert_followers(uid, client, version):
	"""
	Get Followers ids of author from twitter using API.
	uid : User id of the author
	graph_db : neo4j graph 
	client : client twitter API
	version : API version

	"""
	entry = twitter.get_followers(uid, 0, version, client)
	
	if(version==1):
		limit = int(entry['response']['x-ratelimit-remaining'])
	else:
		limit = int(entry['response']['x-rate-limit-remaining'])

	if(entry['response']['status'] != '200'):
		return [entry, limit, 0]
	else:
		return [entry, limit, 1]

def insert_friends(uid, client, version):
	"""
	Get friends ids of author from twitter using API.
	uid : User id of the author
	graph_db : neo4j graph 
	client : client twitter API
	version : API version

	"""
	entry = twitter.get_friends(uid, 0, version, client)
	
	if(version==1):
		limit = int(entry['response']['x-ratelimit-remaining'])
	else:
		limit = int(entry['response']['x-rate-limit-remaining'])

	if(entry['response']['status'] != '200'):
		return [entry, limit, 0]
	else:
		return [entry, limit, 1]

def switch_sleep(i, v, time_elapsed, log, graph_db, author_id):
	time.sleep(1)
	if(i<3):
		i+=1
	else:
		if(v==1):
			v=1.1
			i=0
		else:
			dump_log(log, [ i, v, time_elapsed,  datetime.now(), "sleeping 15 mins"])
			for j in range(60):
				time.sleep(15)
			time_elapsed +=15
			i=0
			if(time_elapsed >= 60):
				v=1
				time_elapsed=0	
	
	return [i, 1.1, time_elapsed]	

def dump_text(loc, dst, log, api_config):
	"""
	Collect the relations and dump in text file.
	"""
	i = 0
	v = 1.1
	time_elapsed = 0
	count_followers = 0
	count_friends = 0
	count_authors = 0
	count_duplicats = 0
	limit = 4
	api_client = setup_client(api_config[i])
	
	f = open(loc, "r")
	f_out_followers = open(dst+"followers.txt", "a")
	f_out_friends = open(dst+"friends.txt", "a")
	
	f_out_count = open(dst+"count.txt", "r")
	current_count = int(f_out_count.readline())
	f_out_count.close()

	
	
	for line in f:
		line = line.replace("\n", "")
		row = line.split("\t")
		uid = row[0]	
		author_id = uid

		count_authors += 1
		f_out_count = open(dst+"count.txt", "w")
		f_out_count.write(str(count_authors))
		f_out_count.close()

		entry_1, limit, status = insert_followers(author_id, api_client, v)
		followers = entry_1['followers']

		entry_2, limit, status = insert_friends(author_id, api_client, v)
		friends = entry_2['friends']
		
		count_followers += len(followers)
		count_friends += len(friends)

		# pprint(entry_1['response'])
		# pprint(entry_2['response'])
		
		if(status == 1):
			# write in the file
			f_out_followers.write(json.dumps(entry_1)+"\n")
			f_out_friends.write(json.dumps(entry_2)+"\n")
			# If the limit is reached
			if(limit <= 1):
				dump_log(log, [loc, count_authors, count_followers, count_friends, datetime.now(), limit,"limit_reached for id : ", i])
				i, v, time_elapsed = switch_sleep(i, v, time_elapsed, log, graph_db, author_id)
				api_client = setup_client(api_config[i])

		# TwitterAPI response is not okay.
		else :
			# If the limit is reached
			if(limit <= 1):
				dump_log(log, [loc, count_authors, count_followers, count_friends, datetime.now(), limit,"limit_reached for id :", i])
				i, v, time_elapsed = switch_sleep(i, v, time_elapsed, log, graph_db, author_id)
				api_client = setup_client(api_config[i])

			else:
				dump_log(log, [loc, count_authors, count_followers, count_friends, datetime.now(), limit," Random Twitter or bad user"])



def neo_graph(loc, log, api_config):
	"""
	Collect and store the twitter relations in Neo4j.
	"""

	i = 0
	v = 1.1
	time_elapsed = 0
	count_followers = 0
	count_friends = 0
	count_authors = 0
	count_duplicats = 0
	limit = 4
	graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
	api_client = setup_client(api_config[i])
	
	f = open(loc, "r")
	for line in f:
		line = line.replace("\n", "")
		row = line.split("\t")
		uid = row[0]	
		author_id = uid
		author = graph_db.get_or_create_indexed_node("users", "uid", str(author_id), {"uid" : str(author_id), "is_author" : '1'})
		
		count_authors +=1
		
		# THE RELATIONS HAVEN'T BEEN RESOLVED YET 
		if(author['tagged'] == None and author['bad'] == None):

			# Sleeping 2 sec to behave nicely with TwitterAPI.
			

			# resp = api_client.request("https://api.twitter.com/1.1/account/verify_credentials.json")

			entry, limit, status = insert_followers(author_id, api_client, v)
			followers = entry['followers']
			# pprint(entry['response'])

			# break
			# pprint(entry_2['response'])

			# Sleeping 2 sec to behave nicely with TwitterAPI.
			# time.sleep(2)
			
			entry, limit, status = insert_friends(author_id, api_client, v)
			friends = entry['friends']
			# pprint(entry['response'])

			count_followers += len(followers)
			count_friends += len(friends)
			
			# print "authors : %d , followers %d , friends %d "%(count_authors, count_followers, count_friends)

			# TwitterAPI response is OKAY and above limit
			if(status == 1):
				# print "limit : %d, appid %d, status %d "%(limit, i, status), "Writing to neo4j..."
				batch = neo4j.WriteBatch(graph_db)
				# author = graph_db.get_indexed_node("users", "uid", str(uid))
				# this should be true as the authors profiles have been created first.
				# 	followers = entry['followers']

				for fid in followers:
					batch.get_or_create_indexed_node("users","uid", fid ,{"uid" : fid,"is_author": "0"})
				nodes = batch.submit()
				# Create the relations
				for n in nodes:
					batch.get_or_create_relationship(n, "follows", author)
				rels = batch.submit()

				for fid in friends:
					batch.get_or_create_indexed_node("users","uid", fid ,{"uid" : fid,"is_author": "0"})
				nodes = batch.submit()
					# Create the relations
				for n in nodes:
					batch.get_or_create_relationship(author, "follows", n)
				rels = batch.submit()
				author['tagged'] = 1

			
				# If the limit is reached
				if(limit <= 1):
					dump_log(log, [loc, count_authors, count_followers, count_friends, datetime.now(), limit,"limit_reached for id : ", i])
					i, v, time_elapsed = switch_sleep(i, v, time_elapsed, log, graph_db, author_id)
					api_client = setup_client(api_config[i])
					graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

			# TwitterAPI response is not okay.
			else :
				# If the limit is reached
				if(limit <= 1):
					dump_log(log, [loc, count_authors, count_followers, count_friends, datetime.now(), limit,"limit_reached for id :", i])
					i, v, time_elapsed = switch_sleep(i, v, time_elapsed, log, graph_db, author_id)
					api_client = setup_client(api_config[i])
					graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

				else:
					dump_log(log, [loc, count_authors, count_followers, count_friends, datetime.now(), limit," Random Twitter..Sleeping 10 sec."])
					author['bad'] = 1 
					# time.sleep(10)

		else:
			count_duplicats += 1
			if(count_duplicats % 10 == 0):
				dump_log(log, [loc, count_authors, count_followers, count_friends, datetime.now(), limit, " duplicates so far", count_duplicats])
				# print "authors : %d , followers %d , friends %d "%(count_authors, count_followers, count_friends),"1000N duplicates passed."

		# if(count_authors == 50):		
		break
	

	dump_log(log, [loc, count_authors, count_followers, count_friends, count_duplicats, datetime.now(), limit, "Finishing.."])	

def main():
	"""
	Resolve follow/following relation for tweets clusters authors.
	Calls TwitterAPI withing Limit and stores in Neo4j.

	"""
	# loc = "/home/pranayag/past_trends/tweets_cluster/final_sorted/jeep patriot.txt"
	# log = "/home/pranayag/past_trends/tweets_cluster/final_sorted/resolve.log"
	
	loc = "/Users/pranayag/mtp/validation/raw_tweets/1/jeep patriot.txt"
	log = "/Users/pranayag/mtp/validation/resolve/log_resolve"
	
	# Location to dump the followers and friends ids.
	# dst = "./"
	
	api_config = setup_config("twitter_app.txt")
	neo_graph(loc, log, api_config)
	# print "\n\n\n"
	# dump_text(loc, dst, log, api_config)

	# mode = 0
	# if(mode == 1):
	# 	try :
	# 		neo_graph(loc, log, api_config)
	# 	except:
	# 		main()
	# else:
	# 	dump_text(loc, dst, log, api_config)
main()		