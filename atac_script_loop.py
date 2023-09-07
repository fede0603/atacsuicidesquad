#pip install --upgrade gtfs-realtime-bindings
#pip2 install --upgrade gtfs-realtime-bindings


#https://romamobilita.it/it/tecnologie
#https://romamobilita.it/sites/default/files/rome_static_gtfs.zip

from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from protobuf_to_dict import protobuf_to_dict
import urllib.request as ur
import pandas as pd
import zipfile
import os
import time

pd.options.mode.chained_assignment = None

def scrape_data():

	
	feed_tu = gtfs_realtime_pb2.FeedMessage()
	feed_vp = gtfs_realtime_pb2.FeedMessage()

	# acquire both feeds
	response_tu = ur.urlopen('https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb')
	response_vp = ur.urlopen('https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb')

	feed_tu.ParseFromString(response_tu.read())
	feed_vp.ParseFromString(response_vp.read())

	# convert to dict from our original protobuf feed
	tu_dict = MessageToDict(feed_tu)
	vp_dict = MessageToDict(feed_vp)

	df_tu_b = pd.json_normalize(tu_dict['entity'])
	df_vp_b = pd.json_normalize(vp_dict['entity'])

	# remove underground trips
	df_tu = df_tu_b[~df_tu_b["tripUpdate.trip.tripId"].str.startswith('VJ',na=False)]
	df_vp = df_vp_b[~df_vp_b["vehicle.trip.tripId"].str.startswith('VJ',na=False) ]

	# identify maximum value of timestamps - Used to define a column and the name of the files
	maxtime_tu=int(df_tu['tripUpdate.timestamp'].astype(float).max())
	maxtime_vp=int(df_vp['vehicle.timestamp'].astype(float).max())

	maxtime = max(maxtime_tu,maxtime_vp)

	df_tu ["max_time"] = maxtime
	df_vp ["max_time"] = maxtime

	file_name_tu="./tu_"+str(maxtime)+".csv" # associate the same timestamp to both files to be able to identify as being the same
	file_name_vp="./vp_"+str(maxtime)+".csv"

	#Write csv

	df_tu.to_csv(f"{data_folder}/{file_name_tu}", header=True, index=None, sep=';', mode='w+')
	df_vp.to_csv(f"{data_folder}/{file_name_vp}", header=True, index=None, sep=';', mode='w+')


	return df_tu, df_vp

max_tries = 5
seconds_for_loop = 60
num_minuti = 150
data_folder = r"./extradata"

for tx in range(0,num_minuti+1):
	print(tx)
	
	t0 = time.time()

	num_tries = 0
	for tries in range(max_tries):
		try:
			df_tu, df_vp = scrape_data()
			break
		except Exception as e:
			print(repr(e))
			print("Received Error, Trying again")
			num_tries +=1
			

		if num_tries >= max_tries:
			print(f"Exceeded {max_tries}, going to next iteration")
			break
		time.sleep(1)
	
	if tx == 0:
		print("Hello")
		df_total_tu = df_tu
		df_total_vp = df_vp
	else:
		df_total_tu = pd.concat([df_total_tu, df_tu])
		df_total_vp = pd.concat([df_total_vp, df_vp])
	run_time = time.time() - t0

	print(tx, run_time)
	time.sleep(max(0.1, seconds_for_loop-run_time))

df_total_tu.to_csv(f"{data_folder}/df_total_tu.csv", header=True, index=None, sep=';')
df_total_vp.to_csv(f"{data_folder}/df_total_vp.csv", header=True, index=None, sep=';')