#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy
#
# make sure to set env variable:
# export GOOGLE_APPLICATION_CREDENTIALS="<path to private json key file>"

import pandas as pd
import datetime as dt
import time as t
import os
from sodapy import Socrata
from google.cloud import storage, pubsub_v1

PROJECT_ID = "chicago-traffic-tracker-demo"
TOPIC_NAME = "chicago-traffic-topic"
BUCKET_NAME = "chicago-traffic-data"
SERVICE_ACCOUNT_ACCESS_KEY = "<replace with path to json key file>"
APP_TOKEN = "<replace with app token>"


def convert_to_msg(pdt):
    index = str(pdt.__dict__["Index"])
    comments = str(pdt.__dict__["_1"])
    direction = pdt.__dict__["_2"]
    from_street = pdt.__dict__["_3"]
    last_updated = pdt.__dict__["_4"]
    length = str(pdt.__dict__["_5"])
    start_lat = str(pdt.__dict__["_6"])
    end_lat = str(pdt.__dict__["_7"])
    end_lon = str(pdt.__dict__["_8"])
    street_heading = str(pdt.__dict__["_9"])
    to_street = pdt.__dict__["_10"]
    speed = str(pdt.__dict__["_11"])
    segment_id = str(pdt.__dict__["segmentid"])
    start_lon = str(pdt.__dict__["start_lon"])
    street = str(pdt.__dict__["street"])

    sep = ","
    msg_str = sep.join([index, last_updated, segment_id, speed, street, from_street, to_street, street_hea
ding, direction, length, start_lat, end_lat, start_lon, end_lon, comments])

    print(msg_str)
    return bytes(msg_str, 'utf-8')

while True:

    # instantiate Socrata client
    socrata_client = Socrata("data.cityofchicago.org", APP_TOKEN, username="<replace with username>", password="<replace with password>")

    # get up to 500,000 results, returned as JSON from API / converted to Python list of dictionaries by sodapy.
    result_list = socrata_client.get("8v9j-bter", limit=500000)
    socrata_client.close()

    # convert to pandas DataFrame
    df = pd.DataFrame.from_records(result_list)

    # instantiate Pub/Sub client
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

    # publish each row as a message
    for pdt in df.itertuples():
        msg_bytes = convert_to_msg(pdt)
        publisher.publish(topic_path, msg_bytes)

    # archive events to Cloud Storage
    ts = t.time()
    timestamp = dt.datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M:%S')
    destination_file = timestamp + ".csv"
    
    #local_file = "/tmp/" + timestamp + ".csv"
    #df.to_csv(local_file)
    
    # upload file to bucket
    storage_client = storage.Client().from_service_account_json(SERVICE_ACCOUNT_ACCESS_KEY)
    bucket = storage_client.get_bucket(BUCKET_NAME)
    blob = bucket.blob(destination_file)
    blob.upload_from_filename(local_file)
    t.sleep(30)
    os.remove(local_file)

    # sleep for 120 seconds before next pull
    print("sleeping for 120 seconds")
    t.sleep(120)
