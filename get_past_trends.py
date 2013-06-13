import urllib2
import json
from pprint import pprint
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

# FETCHES DATA FROM API FOR GIVEN DATE AND TIME
def past_trends_time(date, time, api, loc):
	api_call = api+"/"+str(date)+"/query_recency."+str(time)+".txt"
	f = open(loc+"/"+str(date)+"_"+str(time), "w")
	handle = urllib2.urlopen(api_call)
	response = handle.read()
	# pprint(response)
	# pprint(json.loads(response))
	f.write(response)
	f.close()

def get_trends():
	api = "http://dictionary.timesense.yahoo.com/buzz/en-US"
	loc = "../past_trends"
	log = "../past_trends/trends.log.txt"
	
	time_limit = 2330
	day_limit = 5
	d = 4
	t = 1900
	gap = [30, 70]
	g = 0

	while(d <= day_limit):
		if(t > time_limit):
			t = 0
			d +=1
			g = 0
		date = "2013-06-0"+str(d)
		time_stamp = convert_time(t)

		t += gap[g % 2]
		g += 1
		
		try :
			past_trends_time(date, time_stamp, api, loc)
			dump_log(log, [date, time_stamp, loc+"/"+str(date)+"_"+str(time_stamp), "1", datetime.now()])
		except:
			dump_log(log, [date, time_stamp, loc+"/"+str(date)+"_"+str(time_stamp), "-1", datetime.now()])
			time.sleep(2)
		
		time.sleep(1)

def convert_time(t):
	if(t >= 1000):
		return str(t)
	else:
		if(t >= 100):
			return "0"+str(t)
		else:
			if(t >= 10):
				return "00"+str(t)
			else:
				return "000"+str(t)

get_trends()