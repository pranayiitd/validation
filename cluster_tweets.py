# GET ALL TWEET OBJECT CONTAING THE TOPIC KEYWORDS IN THE TEXT
# A NAIVE TO GET ALL THE TWEETS RELATED TO PARTICULAR TOPIC BY MATCHING PHRASES.

import json
from pprint import pprint
from datetime import datetime
import os

def dump_log(loc, arr):
	f = open(loc, "a")
	s = ""	
	for elem in arr:
		s = s + str(elem) + "\t"
	s = s + "\n"
	f.write(s)
	f.close()

def make_row(arr):	
	s = ""
	for elem in arr:
		s = s + str(elem) + "\t"
	s = s + "\n"
	return s

# Check if the text contains any tuple (but all keys in a tuple) of the array
def has_word(text, arr):
	tf = False
	for variant in arr:
		if(variant in text):
			tf = True
			break
	return tf

def has_token(text, arr):
	tf = False
	text_arr = text.split(" ")
	
	for token in arr :
		if(token in text_arr):
			tf = True
			break
	return tf

def get_processed_filers(log):
	f = open(log, "r")
	processed_filers = []
	for line in f:
		line = line.split("\t")
		processed_filers.append(line[1])
	
	return processed_filers

def cluster_tweet(tobj, phrases_bag, unigrams, clusters):
	text = tobj['rtds_tweet']['text'].encode("ascii","ignore")
	uid = str(tobj['rtds_tweet']['user_id'])
	ts  = str(tobj['rtds_tweet']['created_at'])
	text = text.lower()
	
	# LOOK FOR ALL THE CLUSTERS. STOP AT THE FIRST FOUND CLUSTER.
	for key in phrases_bag.keys():
		if(has_word(text, phrases_bag[key]) or has_token(text, unigrams[key]) ):
			f = clusters[key]
			f.write(make_row([uid, ts, text]))
			break

def tweet_cluster(phrases_bag, unigrams):
	
	clusters = {}
	# LOCATION FOR CLUSTERS DUMP
	
	dst ="/home/pranayag/past_trends/tweets_cluster/final/"
	log ="/home/pranayag/past_trends/tweets_cluster/final/cluster.log"
	
	# dst ="cluster/"
	# log = "cluster/log_.txt"

	processed_filers = get_processed_filers(log)
	
	for key in phrases_bag.keys():
		f = open(dst+str(key)+".txt","a")
		clusters[key] = f

	# SEARCHING THE FILTERED TWEETS TO BE CLUSTERED.

	loc = "/home/y/var/timesense/data/twitter_filteredTweets_rsync/en-US/syc/"
	folders = [ "2013-06-07","2013-06-08", "2013-06-09", "2013-06-10", "2013-06-11" ]

	# loc = "./raw_tweets/"
	# folders = ["2"]
	duplicates = 0
	for folder in folders:
		try:
			files = os.listdir(loc+folder)
			for fil in files:
				# Skip the already done files.
				if(fil in processed_filers):
					duplicates += 1
					dump_log(log, [folder, fil, datetime.now(), "skip duplicates"])
					continue
				
				try:
					f = open(loc+folder+"/"+fil,"r")
					for line in f:
						tobj = json.loads(line)
						cluster_tweet(tobj, phrases_bag, unigrams, clusters)
					f.close()
					dump_log(log, [folder, fil, datetime.now(), "1"])
				
				except:
					dump_log(log, [folder, fil, datetime.now(), "-1"])
					continue
		except:
			dump_log(log, [folder, datetime.now(), "-2"])
			continue
			
	# Closing the file descp
	for fo in clusters.values():
		fo.close()

def main():
	
	f = open("topic_list.txt", "r")
	line = f.readline()
	phrases_bag = {}
	unigrams = {}

	while line:
		key = line.split(",")[0].strip()
		
		line = f.readline()
		line = line.replace('\n','')
		phrases_bag[key] = []
		variants = line.split(",")
		for v in variants:
			if(v != ''):
				phrases_bag[key].append(v.strip().lower())

		line = f.readline()
		line = line.replace('\n', '')
		unigrams[key] = []
		variants = line.split(",")
		for v in variants:
			if(v != ''):
				unigrams[key].append(v.strip().lower())		
		
		line = f.readline()
		line = f.readline()
	try:
		tweet_cluster(phrases_bag, unigrams)
	except : 
		tweet_cluster(phrases_bag, unigrams)

main()	