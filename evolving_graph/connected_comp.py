from py2neo import neo4j, cypher
import json
from pprint import pprint
from datetime import datetime

time_stamp = 2
graph_component = {}
gcount =0
START_TIME = 0
END_TIME = 0

def handle_row(row ):
	node = row[0]
	global time_stamp
	rf = False
	if(node['time_stamp'] == None):
		rf = True
	else:
		if(node['time_stamp'] != time_stamp):
			rf = True
	if(rf == True):
		global graph_component
		cid = len(graph_component)
		node['visited'] = cid
		bfs(node, cid, graph_component, time_stamp)
	else:
		return

def bfs(node, cid, graph_component, time_stamp):
	"""
	Does graph traversal BFS, to find out graph stats.
	"""
	tf = False
	if(node['time_stamp'] == None):
		tf = True
	else:
		if(node['time_stamp'] != time_stamp):
			tf = True
	
	if(tf != True):
		return

	if(graph_component.has_key(cid) == False):
		graph_component[cid] = 1
	else:
		graph_component[cid] += 1

	node['time_stamp'] = time_stamp
	rels = node.get_related_nodes(neo4j.Direction.BOTH, "follows")
	for rel in rels:
		bfs(rel, cid, graph_component, time_stamp)
		
def get_comp_sizes(arr):
	"""
	Returns largest, second largest, median, average of nums in arr
	"""
	arr.sort()
	l = len(arr)
	one = arr[l - 1]
	two = arr[l - 2]
	median = arr[l / 2]
	avg = sum(arr) / l
	
	return[one, two, median, avg, l, sum(arr) - l ]

def handle_row_custom(row):
	"""
	Process each node of the graph.
	"""
	global START_TIME 
	global END_TIME
	node = row[0]
	if(node['ts'] == None):
		return
	node_ts = int(node['ts'])

	time_stamp = END_TIME
	rf = False
	if(node['time_stamp'] == None):
		rf = True
	else:
		if(node['time_stamp'] != time_stamp):
			rf = True

	if(node_ts >= START_TIME and node_ts <= END_TIME and rf):
		global graph_component
		cid = len(graph_component)
		node['visited'] = cid
		bfs_custom(node, cid, graph_component, START_TIME, END_TIME)


def bfs_custom(node, cid, graph_component, START_TIME, END_TIME):
	"""
	Does graph traversal for nodes in the window time range.
	"""
	time_stamp = END_TIME
	tf = False
	if(node['time_stamp'] == None):
		tf = True
	else:
		if(node['time_stamp'] != time_stamp):
			tf = True
	
	node_ts = node['ts']
	
	# IF THE NODE HASN'T BEEN VISITED AND IN THE TIME-RANGE.
	if(node_ts >= START_TIME and node_ts <= END_TIME and tf):
		if(graph_component.has_key(cid) == False):
			graph_component[cid] = 1
		else:
			graph_component[cid] += 1

		node['time_stamp'] = time_stamp
		rels = node.get_related_nodes(neo4j.Direction.BOTH, "follows")
		for rel in rels:
			bfs_custom(rel, cid, graph_component, START_TIME, END_TIME)


def main(ts, START = None, END = None, mode = 1):
	"""
	Does the graph processing at time_stamp ts and returns stats.
	mode = 1 for complete graph traversal
	else  	 for graph traversal within time_window.
	"""
	global time_stamp
	time_stamp = ts

	global graph_component
	# It's necessary to flush the graph_component to remove the last map
	graph_component ={}

	global START_TIME
	START_TIME = START
	global END_TIME
	END_TIME = END
	
	# print time_stamp, START_TIME, END_TIME, graph_component, mode
	# handle_row_custom([2])
	
	# return

	graph_db = neo4j.GraphDatabaseService("http://localhost:7475/db/data/")
	if (mode == 1):
		cypher.execute(graph_db, "START z=node(*) RETURN z", row_handler = handle_row )
	else :
		cypher.execute(graph_db, "START z=node(*) RETURN z", row_handler = handle_row_custom )

	return get_comp_sizes(graph_component.values())
