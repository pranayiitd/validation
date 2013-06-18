from py2neo import neo4j
import json
from pprint import pprint
from datetime import datetime
import connected_comp

def dump_log(loc, arr):
	f = open(loc,"a")
	s = ""	
	for elem in arr:
		s = s + str(elem)+"\t"
	s = s + "\n"
	f.write(s)
	f.close()

def complete_graph(loc, log, source_port, target_port):
	"""
	This will make the complete evolving graph for the topic tweets
	from loc and will time_stamp each node of the graph.
	This considers unique users only once, second occurance of user is
	counted as duplicates
	"""
	graph_db = neo4j.GraphDatabaseService(
		"http://localhost:"+source_port+"/db/data/")
	
	graph_db_topic = neo4j.GraphDatabaseService(
		"http://localhost:"+target_port+"/db/data/")

	tweets_count = 0
	users_count = 0
	duplicate_users = 0
	edges_count = 0

	f = open(loc, "r")
	for line in f :
		tweets_count += 1
		tobj = line.split("\t")
		uid = tobj[0]
		ts = tobj[1]
		n = graph_db_topic.get_indexed_node("users", "uid", str(uid))
		if(n):
			duplicate_users += 1
			if(duplicate_users % 100 == 0):
				dump_log(log, [tweets_count, users_count, edges_count,
								duplicate_users, datetime.now()," Duplicates"])
			continue
		
		n = graph_db.get_indexed_node("users", "uid", str(uid))
		if(n):
			users_count += 1
			profile =  graph_db.get_properties(n)[0]
			friends = n.get_related_nodes(
				neo4j.Direction.OUTGOING, "follows")
			
			profile['ts'] = ts
			
			author = graph_db_topic.get_or_create_indexed_node(
				"users", "uid", uid, profile)
			
			for friend in friends:
					fid = friend["uid"]
					fn = graph_db_topic.get_indexed_node(
						"users", "uid", str(fid))
					# If my friend already present in G(t-1)
					if(fn):
						graph_db_topic.create( (author, "follows", fn))
						edges_count += 1

			if(tweets_count % 100 == 0):
				dump_log(log, [tweets_count, users_count, edges_count,
								datetime.now(), "Check"])

	f.close()

def insert_day_n(loc, day, log_loc):
	"""
	Creates the evolving graph for the tweets cluster of topic 'loc'
	logs the graph stats at log_loc
	"""
	graph_db = neo4j.GraphDatabaseService(
		"http://localhost:7474/db/data/" )
	graph_db_topic = neo4j.GraphDatabaseService(
		"http://localhost:7475/db/data/" )
	c1 =0
	c2 =0
	duplicates = 0
	edges_count = 0
	f = open(loc, "r")
	window = 0

	for line in f:
		# tobj = json.loads(line)
		# id_ = str(tobj['rtds_tweet']['user_id'])
		# id_ = 1107232339
		
		tobj = line.split("\t")
		id_ = tobj[0]
		ts = int(tobj[1])
		# intializing the window with the time_stamp of first tweet
		if(c1 == 0):
			window = ts
		n = graph_db.get_indexed_node("users", "uid", str(id_))
		c1 += 1
		# If that user present in main Graph G
		if(n):
			c2 += 1
			m = graph_db_topic.get_indexed_node("users", "uid", str(id_))
			# Author already present then skip
			if(m):
				duplicates += 1
				continue
			else:
				profile =  graph_db.get_properties(n)[0]
				friends = n.get_related_nodes(
					neo4j.Direction.OUTGOING, "follows")
				
				profile['ts'] = ts
				# Inserting the author if not already in topical Graph g(t)
				author = graph_db_topic.get_or_create_indexed_node(
					"users", "uid", id_, profile)
			
				for friend in friends:
					fid = friend["uid"]
					fn = graph_db_topic.get_indexed_node(
						"users", "uid", str(fid))
					# If my friend already present in G(t-1)
					if(fn):
						graph_db_topic.create( (author, "follows", fn))
						edges_count += 1

		if(c1 % 100 == 0):
			dump_log(log_loc,
				[c1, c2, l1, l2, mid, avg, l, edges_count, datetime.now(), duplicates, "Duplicates"])
		
		if(ts > window + 30 * 60):
			window = ts
			start = datetime.now()
			l1, l2, mid, avg, l = connected_comp.main(ts)
			end = datetime.now()
			dump_log(log_loc,[c1, c2, l1, l2, mid, avg, l, edges_count, start, end, ts])

	f.close()

def main():
	"""
	Creating the topical graph
	"""
	# tweets_loc = "/home/pranayag/neo/cluster/sorted_tweets/1.txt"
	tweets_loc = "/home/pranayag/past_trends/tweets_cluster/final_sorted/mayor bloomberg.txt"
	log = "/home/pranayag/validation/evolving_graph/mayor_log.txt"

	# insert_day_n(tweets_loc, 1, log)
	try:
		# insert_day_n(tweets_loc, 1, log)
		complete_graph(tweets_loc, log, "7474", "7475")
	except :
		main()

main()	
