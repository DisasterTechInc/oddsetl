#!/usr/bin/env python3
import datetime
import pandas as pd
from config import constants
import git
import os
import helpers as helpers 
import numpy as np
import requests
from geojson import Feature, FeatureCollection, Point
import datetime
import yaml

# Set up git repository
g = git.cmd.Git(constants.git_dir)
# Git Pull to keep us up to date
g.pull()

with open(constants.creds, 'r') as f:
    creds = dict(yaml.safe_load(f.read()))

listOfFiles = list()
for (dirpath, dirnames, filenames) in os.walk(constants.daily_reports):
    listOfFiles += [os.path.join(dirpath, myfile) for myfile in filenames]

listOfFiles = list(set(listOfFiles))
files = [fi for fi in listOfFiles if fi.endswith(".csv")]

dates = []
for datafile in files:
    date_time_str = datafile.split("/")[-1].split(".csv")[0]
    date_time_obj = datetime.datetime.strptime(date_time_str, '%m-%d-%Y')
    dates.append(date_time_obj)

current_date = datetime.datetime.today().strftime('%m-%d-%Y %H:%M:%S')
latest_date = max(set(dates))
print(f"Latest data update is: {latest_date}")
print(f"Current date is: {current_date}")
for datafile in files:
    if str(latest_date.strftime('%m-%d-%Y')) in datafile:
        df_jhu = pd.read_csv(datafile)

df_jhu["logInfected"] = 100*(np.log(float(df_jhu["Active"].to_list()[0]))+1) 
df_jhu['Incidence_Rate'] = df_jhu['Incidence_Rate'].astype(float).apply(lambda x: np.round(x, 2))
df_jhu['Case-Fatality_Ratio'] = df_jhu['Case-Fatality_Ratio'].astype(float).apply(lambda x: np.round(x, 2))
df_jhu = df_jhu.rename(columns={"Long_": "Lon", "Lat_JHU": "Lat"})
df_jhu['county'] = df_jhu['Admin2']
df_jhu['source'] = 'Johns Hopkins University'
df_jhu = df_jhu[~df_jhu.county.isna()]
county_loc = df_jhu[['Province_State', 'Lat', 'Lon', 'county']]
county_loc = county_loc[county_loc.Province_State == 'California']
for col in df_jhu.columns:
  df_jhu[col] = [str(x).strip().replace("'", "") for x in df_jhu[col]]

df_hospitals = pd.read_csv("https://data.ca.gov/dataset/529ac907-6ba1-4cb7-9aae-8966fc96aeef/resource/42d33765-20fd-44b8-a978-b083b7542225/download/hospitals_by_county.csv")
current_date = pd.to_datetime('today')
df_hospitals['tdate'] = pd.to_datetime(df_hospitals['todays_date'])
df_hospitals['timedelta'] = abs((df_hospitals['tdate'] - current_date))
min_timedelta = min(df_hospitals['timedelta'])
df_hospitals = df_hospitals[df_hospitals['timedelta'] == min_timedelta]
df_hospitals['source'] = 'California Hospital Association'
df_hospitals['date'] = df_hospitals['todays_date'] 
df_hospitals = pd.merge(county_loc, df_hospitals, on='county')
df_hospitals['Lat'] = df_hospitals['Lat'] + np.random.normal(0,0.02)
df_hospitals = df_hospitals.drop(['tdate','timedelta','todays_date'], axis=1)
df_hospitals['description'] = df_hospitals['county'].apply(lambda x: x + " county data")

df_bed_surge = pd.read_csv("https://data.ca.gov/dataset/cbbfb307-ac91-47ec-95c0-f05684e06065/resource/ef6675e7-cd3a-4762-ba75-2ef78d6dc334/download/bed_surge.csv")
df_bed_surge['tdate'] = pd.to_datetime(df_bed_surge['date'])
df_bed_surge['timedelta'] = abs((df_bed_surge['tdate'] - current_date))
min_timedelta = min(df_bed_surge['timedelta'])
df_bed_surge = df_bed_surge[df_bed_surge['timedelta'] == min_timedelta]
df_bed_surge['source'] = 'California Office of Emergency Services'
df_bed_surge = pd.merge(county_loc, df_bed_surge, on='county')
df_bed_surge['description'] = df_bed_surge['county'].apply(lambda x: x + " county data") 
df_bed_surge['Lat'] = df_bed_surge['Lat'] + np.random.normal(0,0.02)
df_bed_surge = df_bed_surge.drop(['tdate','timedelta'], axis=1)

dfs = {'JHU': df_jhu, 'CHA': df_hospitals, 'COES': df_bed_surge}
for dataframe in dfs.keys():
    df = dfs[dataframe]
    cols = df.keys().to_list()
    geojsonout = helpers.df_to_geojson(df, cols, lat="Lat", lon="Lon")
    geojsonstring = str(geojsonout).replace("'",'"').replace("nan","0")
    geojsonbytes = bytes(geojsonstring, 'utf-8')
    helpers.store_blob_in_odds(geojsonbytes, 
            token = creds['TOKEN'], 
            connectionString = creds['connectionString'], 
            containerName = "jhucovid", 
            blobName = f"covid_{dataframe}.json")



