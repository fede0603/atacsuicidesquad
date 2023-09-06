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

pd.options.mode.chained_assignment = None

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

file_name_tuz="./tu_"+str(maxtime)+".zip" # associate the same timestamp to both files to be able to identify as being the same
file_name_vpz="./vp_"+str(maxtime)+".zip"

#Write csv
df_tu.to_csv(file_name_tu, header=True, index=None, sep=';', mode='w+')
df_vp.to_csv(file_name_vp, header=True, index=None, sep=';', mode='w+')

#Zip csv
with zipfile.ZipFile(file_name_tuz, mode="w") as archive:
	archive.write(file_name_tu,compress_type=zipfile.ZIP_DEFLATED)

with zipfile.ZipFile(file_name_vpz, mode="w") as archive:
	archive.write(file_name_vp,compress_type=zipfile.ZIP_DEFLATED)

#Delete original csv
os.remove(file_name_tu)
os.remove(file_name_vp)

