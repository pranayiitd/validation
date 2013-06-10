# GET ALL TWEET OBJECT CONTAING THE TOPIC KEYWORDS IN THE TEXT
# A NAIVE TO GET ALL THE TWEETS RELATED TO PARTICULAR TOPIC BY MATCHING PHRASES.

import json
from pprint import pprint
from datetime import datetime
import os

def make_row(arr):	
	s =""
	for elem in arr:
		s = s+str(elem)+"\t"
	s = s+"\n"
	return s



# Check if the text contains any tuple (but all keys in a tuple) of the array
def has_word(text, arr):
	tf = False
	for variant in arr:
		if(text.lower().find(variant.lower()) > 0):
			tf = True
			break
	return tf


def cluster_tweet(tobj, phrases_bag, clusters):
	text = tobj['rtds_tweet']['text'].encode("ascii","ignore")
	uid = str(tobj['rtds_tweet']['user_id'])
	ts  = str(tobj['rtds_tweet']['created_at'])

	# Look for all the clusters.
	for key in phrases_bag.keys():
		if(has_word(text, phrases_bag[key])):
			f = clusters[key]
			f.write(make_row([uid, ts, text]))


def tweet_cluster(phrases_bag):
	clusters = {}
	# LOCATION FOR CLUSTERS DUMP
	# dst ="/home/pranayag/neo/cluster/"
	dst ="cluster/"
	for key in phrases_bag.keys():
		f = open(dst+str(key)+".txt","w")
		clusters[key] = f

	# SEARCHING THE FILTERED TWEETS TO BE CLUSTERED.
	# loc = "/home/y/var/timesense/data/twitter_rawTweets/en-US/syc/"
	# folders =["2013-06-05","2013-06-06","2013-06-07"]
	

	loc = "./raw_tweets/"
	folders = ["1"]
	for folder in folders:
		files = os.listdir(loc+folder)
		for fil in files:
			f = open(loc+folder+"/"+fil,"r")
			line = f.readline()
			while line:
				tobj = json.loads(line)
				cluster_tweet(tobj, phrases_bag, clusters)
				line = f.readline()
			f.close()

	# Closing the file descp
	for fo in clusters.values():
		fo.close()


def main():
	
	f = open("topic_list.txt", "r")
	line = f.readline()
	phrases_bag = {}

	while line:
		key = line.split(",")[0].strip()
		line = f.readline()
		line = line.replace('\n','')
		phrases_bag[key] = []
		variants = line.split(",")
		for v in variants:
			if(v!=''):
				phrases_bag[key].append(v.strip())
		
		line = f.readline()
		line = f.readline()

	tweet_cluster(phrases_bag)

main()	