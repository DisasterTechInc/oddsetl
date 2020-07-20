import pandas as pd
from config import constants
import os
import sys
from sodapy import Socrata
import subprocess
import yaml
from azure.storage.blob import BlobClient

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("data.kcmo.org", None)

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results = client.get("7at3-sxhp", limit=2000)

with open(constants.creds, 'r') as f:
    creds = dict(yaml.safe_load(f.read()))

def store_blob_in_odds(data, token, connectionString, containerName, blobName):
    """Store json files in db."""

    blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=blobName)
    blob.upload_blob(data, overwrite=True)
    print(f'Data uploaded in Azure: {blobName}')

    return

def df_to_geojson(df):
    geojson = {'type':'FeatureCollection', 'features':[]}
    for _, row in df.iterrows():
        feature = {'type':'Feature',
                   'properties':{},
                   'geometry':{'type':'Point',
                               'coordinates':[]}}
        feature['geometry']['coordinates'] = [float(row['Lon']),float(row['Lat'])]
        for prop in df.keys().to_list():
            feature['properties'][prop] = row[prop]
        geojson['features'].append(feature)
    return geojson

df = pd.DataFrame.from_records(results)
request_types = list(df['request_type'].unique())
tree_requests = [k for k in request_types if 'Trees-Trimming-Block Pruning' in k]
df = df[df.request_type.isin(tree_requests)]
df = df[['case_id', 'request_type',
       'type', 'detail', 'creation_date', 
       'status', 'exceeded_est_timeframe',
       'street_address', 'zip_code', 'neighborhood',
       'ycoordinate', 'xcoordinate', 'case_url', 'days_open',
       'closed_date']]
df = df.rename(columns={'xcoordinate': 'Lat', 'ycoordinate': 'Lon'})
geojsonout = df_to_geojson(df)
geojsonstring = str(geojsonout).replace("'",'"').replace("nan","0")
geojsonbytes = bytes(geojsonstring, 'utf-8')
store_blob_in_odds(geojsonbytes,
        token = creds['TOKEN'],
        connectionString = creds['connectionString'],
        containerName = "hexstream",
        blobName = f"hexstream_kstrees.json")


