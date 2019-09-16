#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
import datetime as dt
import time as t
from sodapy import Socrata

ts = t.time()
timestamp = dt.datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M:%S')
filename = "/home/shirley_cohen/data/" + timestamp + ".csv"

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
# client = Socrata("data.cityofchicago.org", None)
MyAppToken = "fvXvcsQ8qpzOM5yGZEwgjqu1Q"

# authenticated client
client = Socrata("data.cityofchicago.org", MyAppToken, username="replace with email", password="replace with password")

# Get 5000 results, returned as JSON from API / converted to Python list of dictionaries by sodapy.
#result_list = client.get("8v9j-bter", limit=5000)
result_list = client.get("8v9j-bter", limit=50000)

# Convert to pandas DataFrame
df = pd.DataFrame.from_records(result_list)
#print("head: " + str(df.head()))
df.to_csv(filename)

#for row in df.itertuples():
#       print(row)

client.close()
