import json
import os 
from datetime import datetime
import time 

def dump_log(loc, arr):
	f = open(loc,"a")
	s =""	
	for elem in arr:
		s = s+str(elem)+"\t"
	s = s+"\n"
	f.write(s)
	f.close()

# GENERATES ALL THE BIGRAMES FOR THE GIVEN PHRASE
def generate_bigrams(phrase):
	tokens = phrase.split(" ")
	bigrams = []
	for i in range(len(tokens)):
		if(i +1 < len(tokens)):
			bigrams.append(tokens[i]+ " " + tokens[i+1])
		else:
			if(i == 0):
				bigrams.append(tokens[i])
	return bigrams

def bigram_cluser(bigrams_set, bigram, c1, c2, bigrams_score, score):
	tf  = False
	
	for i in range(len(bigrams_set)):
		# Found a similar cluster
		if(bigrams_set[i] & set(bigram) != set()):
			# Merging of cluster results in Chaining which leads to large set of bigrams.
			# bigrams_set[i] = bigrams_set[i] | set(bigram)

			# Get the bigram with higer score
			if(score > bigrams_score[i]):
				bigrams_set[i] = set(bigram)
			bigrams_score[i] = max(bigrams_score[i], score)
			c1+=1
			tf = True
			break
	
	# Creating a new cluster
	if(tf == False):
		bigrams_set.append(set(bigram))
		bigrams_score.append(score)
		if(len(bigrams_set) != len(bigrams_score)):
			c1 = -111111
			c2 = -111111
		c2 +=1

	return [c1, c2]

def main():

	flist = os.listdir("../past_trends/raw_topics/")
	log = "../past_trends/trends.log.txt"
	for fl in flist:
		loc = "../past_trends/raw_topics/"+fl
		dst = "../past_trends/cluster_topics/"+fl
		
		# print loc, dst

		# break
		f = open(loc, "r")
		line = f.readline()
		line = f.readline()
		line = f.readline()

		# bigrams_set = [set(["dummy"]),set(["dummy", "rwby"])]
		bigrams_set =[]
		bigrams_score =[]

		c1 =0; c2 =0

		while line:
			arr = line.split("\t")
			q = arr[0]
			qn = arr[1]
			count = arr[2]
			score = arr[3]
			c1, c2 = bigram_cluser(bigrams_set, generate_bigrams(qn), c1, c2, bigrams_score, score)
			
			if(c1 < 0):
				print "LENGTH CAN'T BE DIFFERENT////"
				break
			# break
			line  = f.readline()

		f.close()
		bigrams_set_sorted = []
		for i in range(len(bigrams_set)):
			bigrams_set_sorted.append([bigrams_set[i], bigrams_score[i]])

		bigrams_set_sorted.sort(key = lambda tup:tup[1], reverse =True)


		f = open(dst, "w")
		for i in range(len(bigrams_set)):
			f.write(json.dumps(list(bigrams_set_sorted[i][0]))+"\t"+str(bigrams_set_sorted[i][1]) +"\n")

		f.close()

		dump_log(log, [fl, c1, c2, datetime.now()])

main()	