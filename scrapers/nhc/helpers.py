from sys import version_info
import shutil
import pytz
import datetime as dt
import os
from config import logger_config, constants
import geojson
import json
import mysql.connector
import requests, zipfile, io
import reverse_geocoder as rg
from bs4 import BeautifulSoup
import subprocess
import uuid
from azure.storage.blob import BlobClient
import logging

def cleanup_the_house():
    shutil.rmtree(f'{constants.data_dir}/')
    shutil.rmtree(f'{constants.output_dir}/')

def make_dirs(tropical_storms):

    if os.path.exists(f'{constants.output_dir}/'):
        shutil.rmtree(f'{constants.output_dir}/')
    if os.path.exists(f'{constants.data_dir}/'):
        shutil.rmtree(f'{constants.data_dir}/')

    os.mkdir(f'{constants.output_dir}/')
    os.mkdir(f'{constants.data_dir}/')
    
    for tropical_storm in tropical_storms:
        if not os.path.exists(f'{constants.data_dir}/{tropical_storm.lower()}'):
            os.mkdir(f'{constants.data_dir}/{tropical_storm.lower()}')
            os.mkdir(f'{constants.data_dir}/{tropical_storm.lower()}/forecasts')
            os.mkdir(f'{constants.data_dir}/{tropical_storm.lower()}/tracks')

    return

def validate_inputs(params):
    """Validate inputs. """
    errors = []

    if not version_info[0] > 2:
        errors.append('Python 3+ is required to run pipeline!')

    if type(params['main_args']['upload']) != bool:
        errors.append('upload argument should be True or False')

    if type(params['main_args']['year']) != str:
        errors.append('year argument should be a string')

    if int(params['main_args']['year']) < 2008:
        errors.append('year should be 2008 or later')

    if int(params['main_args']['year']) > 2020:
        errors.append('year should be 2020 or earlier')

    if type(params['main_args']['storms_to_get']) != str:
        errors.append('storms_to_get argument should be a string')

    erroneous_storms = []
    if params['main_args']['storms_to_get'] not in ["active", "all"]:
        for storm in [params['main_args']['storms_to_get']]:
            if storm.upper() not in params['all_nhc_storms']:
                erroneous_storms.append(storm)
                errors.append(f"You are requesting data for storms that don't exist in NHC database: {erroneous_storms}!")

    if type(params['main_args']['odds_container']) != str:
        errors.append('odds_container should be a string')

    if params['main_args']['odds_container'] not in ["odds", "testcontainer", "nhc", "demos"]:
        errors.append('odds_container chosen is not allowed, please use "odds", "testcontainer", "nhc" or "demos" ')
    if errors:
        logger.info(f'Error: {errors}')

    return errors


def find_files(logger, url):
    soup = BeautifulSoup(requests.get(url).text, features="html.parser")

    links = []
    for element in soup.find_all('a'):
        links.append(element.get_text())
    
    return links


def get_active_storms(logger, url):
    """ Scrape NHC main TS page, get list of active tropical storms."""

    soup = BeautifulSoup(requests.get(url).text, features="html.parser")

    contents = []
    for element in soup.find_all('td'):
        contents.append(element.get_text())

    regions = ["Atlantic", "Central North Pacific", "Eastern North Pacific"]
    active_storms = {}
    for elem in contents:
        for region in regions:
            if f"no tropical cyclones in the {region}" in elem:
                active_storms[region] = False

    return active_storms


def get_storms(logger, url):
    """ Scrape NHC url, parse and retrieve Tropical storms from content."""
    
    soup = BeautifulSoup(requests.get(url).text, features="html.parser")

    contents = []
    for element in soup.find_all('td'):
        contents.append(element.get_text())

    output = []
    for content in contents:
        content = content.replace(" ", "_")
        if "Tropical" in content:
            content = content.split("_")[-1]
            output.append(content)
    
    return output


def get_links(logger, url):
    """ Get list of links to download from NHC. """

    list_of_links = find_files(logger, url)
    forecasts, tracks = [], []
    for link in list_of_links:
        if '.zip' in link:
            forecasts.append(f'https://www.nhc.noaa.gov/gis/forecast/archive/{link}')
        elif '.kmz' in link:
            tracks.append(f'https://www.nhc.noaa.gov/storm_graphics/api/{link}')

    return forecasts, tracks


def convert_to_geojson(logger, params, directory, storm, is_active):
    """Convert a kml or shapefile to a geojson file, and output in corresponding datadir."""

    listOfFiles = list()
    for (dirpath, dirnames, filenames) in os.walk(directory):
        listOfFiles += [os.path.join(dirpath, myfile) for myfile in filenames]
    
    listOfFiles = list(set(listOfFiles))
    files = [fi for fi in listOfFiles if fi.endswith(".kml") or fi.endswith(".shp")]

    last_active_files = []
    if not is_active:
        # get last file of hurricane, for hurricanes that ended, to extract the end date and calculate "progress" metric.
        nums = []
        for myfile in files:
            if '.shp' in myfile:
                num = myfile.split('/')[-1].split("-")[-1].split("_")[0]
            else:
                num = myfile.split('/')[-1].split("_")[1].split("_")[0]
       
            suffixes = ["A", "Adv", "adv"]
            for suffix in suffixes:
                if suffix in num:
                    num = num.replace(suffix,"")
            newnum = params['num_mappings'][num] 
            nums.append(newnum)
        
            if nums:
                maxnums = max(list(set(nums)))
            
            for myfile in files:
                if '.shp' in myfile:
                    if str(maxnums) in str(myfile.split('/')[-1].split("_")[0].split("-")[-1]):
                        last_active_files.append(myfile)
                elif ".kml" in myfile:
                    if str(maxnums) in str(myfile.split('/')[-1].split("-")[0].split("-")[-1]):
                        last_active_files.append(myfile)
                else:
                    logger.info(f"No kml or shp file in {myfile}")

    files = files + last_active_files
    for myfile in files:
        #newfile = myfile.split('/')[-1]
        if '.kml' in myfile:
            #os.rename(myfile, f"{constants.output_dir}/nhc_{storm}_{newfile}")
            #myfile = f"{constants.output_dir}/nhc_{storm}_{newfile}"
            bashCommand = f"k2g {myfile} {constants.output_dir}"
        elif ".shp" in myfile:
            #filename = f"{constants.output_dir}/nhc_{storm}_{newfile}"
            bashCommand = f"ogr2ogr -f GeoJSON {constants.output_dir}/{myfile.split('/')[-1]} {myfile}"
        try:
            process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()
        except:
            logger.info(f"couldn't convert {myfile}")

    data_list = []
    files = os.listdir(constants.output_dir)
    for myfile in files:
        if ".geojson" in myfile:
            data_list.append(myfile)
    
    return data_list, last_active_files


def get_last_active_day(params, tropical_storm, last_active_tracks):
    for datafile in last_active_tracks:
        features = get_features(params, tropical_storm, datafile)
    
    print(features)
    print(last_active_tracks)
    import pdb;pdb.set_trace()
    last_active_day = features['datadate_iso']

    return last_active_day 


def get_data_from_url(logger, upload, to_download, directory):
    """Given a list of links to download, get data from urls."""

    if upload == 'active':
        if 'tracks' in directory:
            to_download = to_download[-2:] 
        else:
            to_download = [to_download[-1]] 

    for link in to_download:
        name = link.split("/")[-1].split(".")[0]
        r = requests.get(link)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(f'{directory}/{name}')
    
    return 

def get_weather_outlooks(url):
    soup = BeautifulSoup(requests.get(url).text, features="html.parser")
    contents = []
    for element in soup.find_all('pre'):
        elem = element.get_text().replace('\n', ' ').replace('\r', '') 
        contents.append(elem)

    return contents 

def store_blob_in_odds(logger, params, datafile, token, connectionString, containerName, blobName):
    """Store json files in db."""
    
    blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=blobName)
    with open(datafile, 'rb') as f:
        blob.upload_blob(f, overwrite=True)

    headers = {"Authorization": "Bearer %s" %token, "content-type":"application/json"}
    logger.info(f"Upload successful to odds.{containerName}: {blobName}")

def reverseGeocode(coordinates): 
    result = rg.search(coordinates) 
    return result

def get_features(params, tropical_storm, datafile):
    """."""    
    
    features_dict = {}
    gj = None
    with open(datafile) as f:
        try:
            gj = geojson.load(f)
        except:
            print('Could not load geojson')
    
    if gj:
        features = gj['features'][0]
        datatype = datafile.split("_")[-1].split(".")[0]
        datadate = features['properties'][params['datatype_mappings'][datatype]]
        if datatype in ["WW", "TRACK"]:
            lat = features['geometry']['coordinates'][0][1]
            lon = features['geometry']['coordinates'][0][0]
        elif datatype in ["CONE"]:
            lat = features['geometry']['coordinates'][0][0][1]
            lon = features['geometry']['coordinates'][0][0][0]
        
        location = reverseGeocode(coordinates=[lat, lon])
        datadate_iso = f"{datadate.split(' ')[6]}-{params['months'][datadate.split(' ')[4]]}-{datadate.split(' ')[5]}"
        datadate_time = ''.join(datadate.split(' ')[0:2])
        datadate_time = params['hours'][datadate_time]
        datadate_iso = f"{datadate_iso}{datadate_time}"
        features_dict = {
            'state': location[0]['admin1'],
            'county': location[0]['admin2'],
            'storm_name': tropical_storm,
            'storm_region': params[params['main_args']['year']][tropical_storm]['code'][0:2],
            'storm_code': params[params['main_args']['year']][tropical_storm]['code'],
            'storm_type': params[params['main_args']['year']][tropical_storm]['type'],
            'datadate_iso': datadate_iso,
            'year': datadate.split(' ')[6],
            'timezone': 'UTC',
            'datatype': datafile.split("_")[-1].split(".geojson")[0],
            }
    
    return features_dict

def store_in_json(data, filename):
    with open(f"{constants.output_dir}/{filename}.json", 'w') as f:
        json.dump(data, f)

    return

def insert_in_db(logger, creds, features):
    """ Insert active storms in odds table."""
    conn = mysql.connector.connect(
            host=creds['azure']['host'],
            user=creds['azure']['user'],
	    password=creds['azure']['password'],
	    db=creds['azure']['db'],
	    port=3306)

    cursor = conn.cursor()
    guid = str(uuid.uuid4())
    q = f"INSERT INTO {creds['azure']['oddsdb_table']} (guid, datadate_iso, county, state, storm_name, storm_code, storm_type, year, storm_region, datatype) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);" 
    cursor.execute(q, (guid, features['datadate_iso'], features['county'], features['state'], features['storm_name'], features['storm_code'], features['storm_type'], features['year'], features['storm_region'], features['datatype'],)) 
    conn.commit()
    conn.close()

