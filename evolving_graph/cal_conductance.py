from py2neo import neo4j
import json
from pprint import pprint
from datetime import datetime
import time

# Global variables
start_time = 0
end_time = 0
external_edges_count = 0
internal_edges_count = 0

graph_db_big = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

def dump_log(loc, arr):
	f = open(loc,"a")
	s = ""	
	for elem in arr:
		s = s + str(elem)+"\t"
	s = s + "\n"
	f.write(s)
	f.close()

def handle_row(row):
	"""
	handling each node to get it's friends count from original graph
	"""
	node = row[0]
	uid = node['uid']
	n = graph_db_big.get_indexed_node("users", "uid", str(uid))

	if(n):
		# Relations from original graph (external edges)
		friends = n.get_related_nodes(
					neo4j.Direction.OUTGOING, "follows")
		node['friends_count'] = len(friends)
	else:
		node['friends_count'] = 0

def cal_external_edges():
	"""
	For each node in topical graph get the total no. of friends rels
	they have in the original graph and store in the node.
	"""
	graph_db = neo4j.GraphDatabaseService("http://localhost:7475/db/data/")
	cypher.execute(graph_db, 
		"START z=node(*) RETURN z", row_handler = handle_row )

def handle_row_conductance(row):
	"""
	calculate internal edges and external_edges in this time window.
	"""
	node = row[0]
	ts = node['ts']
	
	global start_time
	global end_time
	global internal_edges_count
	global external_edges_count

	# Take this edge if the source node(hence out-going edge) created in time window.
	if(ts >= start_time and ts <= end_time):
		# All the edges which in in the topical graph(Internal edge)
		friends = node.get_related_nodes(
					neo4j.Direction.OUTGOING, "follows")
		internal_edges_count += len(friends)
		external_edges_count += node['friends_count']

def conductance_window(start, end):
	"""
	calculate the conductance of the graph in this window.
	"""
	global start_time 
	start_time = start
	
	global end_time
	end_time = end
	
	graph_db = neo4j.GraphDatabaseService("http://localhost:7475/db/data/")
	cypher.execute(graph_db, "START z=node(*) RETURN z", row_handler = handle_row_conductance )
	
	global internal_edges_count
	global external_edges_count

	return [internal_edges_count, external_edges_count]

def cal_conductance():
	"""
	calculate the conductance of the topic graph in each time window.
	from the first_tweet to last_tweet.
	"""
	tweet_window_map = {
	
	"jeep" : [1370390351, 1370906392],
	"mark_appel" : [1370394609, 1370907519],
	"matta" : [1370393222, 1370892182],
	"mayor" : [1370390881, 1370908452],
	"jolie" : [1368489378, 1368575701],
	
	}

	topic = tweet_window_map['jeep']
	loc = "/home/pranayag/validation/evolving_graph/"+topic+"_conductance.txt"
	first_tweet = topic[0]
	last_tweet = topic[1]
	window = 30 * 60 

	l1 = first_tweet
	l2 = first_tweet + window

	while (l2 < last_tweet):
		t1 = time.ctime(l1)
		t2 = time.ctime(l2)
		arr = conductance_window(l1, l2)
		dump_log(loc, arr+[datetime.now(), t1, t2])
		l2 += window
