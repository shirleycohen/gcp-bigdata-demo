#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
import datetime as dt
import time as t
import os
from sodapy import Socrata
from google.cloud import storage

BUCKET_NAME = "chicago-traffic-data"
SERVICE_ACCOUNT_ACCESS_KEY = "/home/shirley_cohen/traffic-demo-5c701f9abafe.json"

ts = t.time()
timestamp = dt.datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M:%S')
local_file = "/home/shirley_cohen/data/" + timestamp + ".csv"
destination_file = timestamp + ".csv"
print("writing to file: " + destination_file)

APP_TOKEN = "fvXvcsQ8qpzOM5yGZEwgjqu1Q"

# authenticated Socrata client
socrata_client = Socrata("data.cityofchicago.org", APP_TOKEN, username="shirley.cohen@gmail.com", password="K/
+z2;g;a<6g6qk-")

# Get 500,000 results, returned as JSON from API / converted to Python list of dictionaries by sodapy.
result_list = socrata_client.get("8v9j-bter", limit=500000)

# Convert to pandas DataFrame
df = pd.DataFrame.from_records(result_list)
df.to_csv(local_file)
socrata_client.close()

# upload file to bucket
storage_client = storage.Client().from_service_account_json(SERVICE_ACCOUNT_ACCESS_KEY)
bucket = storage_client.get_bucket(BUCKET_NAME)
blob = bucket.blob(destination_file)
blob.upload_from_filename(local_file)
t.sleep(30)
os.remove(local_file)
