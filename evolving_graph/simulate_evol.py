from py2neo import neo4j
import json
from pprint import pprint
from datetime import datetime
import connected_comp
import time

def dump_log(loc, arr):
	f = open(loc,"a")
	s = ""	
	for elem in arr:
		s = s + str(elem)+"\t"
	s = s + "\n"
	f.write(s)
	f.close()


def main():
	loc = "/home/pranayag/validation/evolving_graph/mark_stats.txt"
	first_tweet = 1370394609 
	last_tweet = 1370907519
	window = 30 * 60 

	l1 = first_tweet
	l2 = first_tweet + window

	while(l2 < last_tweet):
		t1 = time.ctime(l1)
		t2 = time.ctime(l2)
		arr = connected_comp.main(1, START = l1, END = l2 , mode = 2)
		dump_log(loc, arr + [datetime.now(), t1, "to" ,t2])
		l2 += window
main()