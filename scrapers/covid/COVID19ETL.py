#!/usr/bin/env python3


################################################################################
#
# COVID-19 Coronavirus Dataset Extraction, Transformation, and Loading
#
# Extraction from:
#			Johns Hopkins University
#
# Transformation:
# 			Binning and Averaging
#
# Loading:
#			Kafka
#
################################################################################
#
# Setup and Declarations:

import pandas as pd
import git
import os
import ETL_funcs as ETL
import numpy as np
import requests
from azure.storage.blob import ContainerClient, BlobClient
from geojson import Feature, FeatureCollection, Point

################################################################################
# Extraction:
# Source: https://github.com/CryptoKass/ncov-data

# Setup and Declarations:

data_dir	   = "/home/dti/data/"
git_dir		   = data_dir+"/COVID-19/JHU/"
daily_reports	   = "/csse_covid_19_data/csse_covid_19_daily_reports/"

# Set up git repository
g = git.cmd.Git(git_dir)

# Git Pull to keep us up to date
g.pull()

# List the Files
directory_list = os.listdir(git_dir+daily_reports)
directory_list.sort()

# Make a list of the CSVs
csv_list = []
for file in directory_list:
  if "csv" in file:
    csv_list.append(file)

# Import them as Pandas frames
frames_list = []
for csv in csv_list:
  frames_list.append(pd.read_csv(git_dir + daily_reports + csv))

################################################################################
# Transformation:

numInfected = frames_list[-1]["Active"]
logInfected = 100*(np.log(float(numInfected.to_list()[0]))+1)
frames_list[-1]["logInfected"] =  logInfected
      

################################################################################
# Load: 
# convert to geojson
finalFrame = frames_list[-1]
for column in finalFrame.columns:
  finalFrame[column] = [str(x).strip().replace("'", "") for x in finalFrame[column]]
cols = finalFrame.keys().to_list()
geojsonout = ETL.df_to_geojson(finalFrame, cols, lat="Lat", lon="Long_")
geojsonstring = str(geojsonout).replace("'",'"').replace("nan","0")
#print(geojsonstring)
geojsonbytes = bytes(geojsonstring, 'utf-8')
upload_data = geojsonbytes

connectionString = ""
containerName = "jhucovid"
blobName = "jhucovidcurrentgeo.json"
# create container
#container_client = ContainerClient.from_connection_string(conn_str=connectionString, container_name=containerName)
#container_client.create_container()

# publish to container
blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=blobName)
blob.upload_blob(upload_data, overwrite=True)


jsonout = {"ownerid": "Johns Hopkins University",
	      "name": "COVID-19 Cases",
       "attribution": "JHU CSSE",
            "server": "Azure",
           "address": containerName+"/"+blobName,
           "version": csv_list[-1][:-4],
            "access": "public",
"rendering_defaults": "None"}

print(jsonout)

guid="7fba544b-8bcf-479d-ab9e-2a1decf6dbeb"

f = open("/home/dti/tokens/odds.txt", "r")
TOKEN = f.read()
f.close()

headers = {"Authorization": "Bearer %s" %TOKEN,
          "content-type":"application/json"}
response = requests.put('https://odds.disastertech.com/'+guid, data=str(jsonout).replace("\'","\""), headers=headers)
print(response)

print("data uploaded")
