from sys import version_info
import shutil
import pytz
import datetime as dt
import os
from config import logger_config, constants
import geojson
import json
import mysql.connector
from zipfile import ZipFile
import requests, zipfile, io
import reverse_geocoder as rg
from bs4 import BeautifulSoup
import subprocess
import uuid
from azure.storage.blob import BlobClient
import urllib.request

def cleanup_the_house():
    shutil.rmtree(f'{constants.data_dir}/')
    shutil.rmtree(f'{constants.output_dir}/')


def make_dirs(tropical_storms):

    for tropical_storm in tropical_storms:
        if not os.path.exists(f'{constants.data_dir}/{tropical_storm.lower()}'):
            os.mkdir(f'{constants.data_dir}/{tropical_storm.lower()}')
            os.mkdir(f'{constants.data_dir}/{tropical_storm.lower()}/forecasts')
            os.mkdir(f'{constants.data_dir}/{tropical_storm.lower()}/tracks')

    return


def validate_inputs(logger, params):
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

    erroneous_storms, storms_to_get = [], []
    if params['main_args']['storms_to_get'] not in ["active", "all"]:
        storms_to_get = params['main_args']['storms_to_get'].upper().replace(' ','').split(',')
        for storm in storms_to_get:
            if storm not in params['all_nhc_storms']:
                erroneous_storms.append(storm)
                errors.append(f"You are requesting data for storms that don't exist in NHC database: {erroneous_storms}!")
    else:
        storms_to_get = params['main_args']['storms_to_get']

    if type(params['main_args']['odds_container']) != str:
        errors.append('odds_container should be a string')

    if params['main_args']['odds_container'] not in ["oddsetldevtest", "nhc", "demos"]:
        errors.append('odds_container chosen is not allowed, please use "odds", "testcontainer", "nhc" or "demos" ')
    if errors:
        logger.info(f'Error: {errors}')

    return errors, storms_to_get


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
    contents = str(contents).replace('\n','')
    active_storm_regions = []
    storms = [] 
    for region in regions:
        if not f"There are no tropical cyclones in the {region} at this time" in contents:
           active_storm_regions.append(region) 
           storms.append(contents.split("Satellite")[0].split("Tropical Storm ")[-1].strip(r"\n").upper())

    return active_storm_regions, storms


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


def get_stormspan_files(params, files):

    first_active_files, last_active_files = [], []
    
    nums = []
    filedict = {}
    for myfile in files:
        num = myfile.split('/')[-1].split("_")[1].split("_")[0]
        suffixes = ["A", "Adv", "adv"]
        for suffix in suffixes:
            if suffix in num:
                num = num.replace(suffix,"")
        newnum = params['num_mappings'][num]
        filedict[myfile] = newnum
        nums.append(newnum)

    if nums:
        maxnum = max(list(set(nums)))
        minnum = min(list(set(nums)))
        
        for myfile in files:
            if maxnum == filedict[myfile]:
                last_active_files.append(myfile)
            if minnum == filedict[myfile]:
                first_active_files.append(myfile)
    
    return first_active_files, last_active_files


def convert_to_geojson(logger, params, directory, storm, is_active):
    """Convert a kml or shapefile to a geojson file, and output in corresponding datadir."""

    listOfFiles = list()
    for (dirpath, dirnames, filenames) in os.walk(directory):
        listOfFiles += [os.path.join(dirpath, myfile) for myfile in filenames]
    
    listOfFiles = list(set(listOfFiles))
    files = [fi for fi in listOfFiles if fi.endswith(".kml") or fi.endswith(".shp")]

    for myfile in files:
        if '.kml' in myfile:
            bashCommand = f"k2g {myfile} {constants.output_dir}"
        elif ".shp" in myfile:
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
    
    return data_list


def convert_EDTdate_to_isoformat(params, date):
    
    date = date.replace("/", " ")
    datesplit = date.split(" ")
    time, datadate_iso, timezone, year = '', '', '', None
    
    if len(datesplit) == 3:
        datadate_iso = f"{datesplit[0]}"
        time = f"{datesplit[1]}"
        year = f"20{datesplit[0][0:2]}"
        timezone = f"{datesplit[2]}" 
    elif len(datesplit) == 7:
        datadate_iso = f"{date.split(' ')[6]}-{params['months'][date.split(' ')[4]]}-{date.split(' ')[5]}"
        time = ''.join(date.split(' ')[0:2])
        year = date.split(' ')[6]
        timezone = ''
 
    if timezone in ["EDT","EST","AST"]:
        time = params['hours'][time]
    else:
        time = f"{time}T"

    date_iso = f"{datadate_iso}{time}_{timezone}"

    return date_iso, year


def get_storm_timeline(params, tropical_storm, is_active, files):
    
    storm_timeline = {}
    first_active_files, last_active_files = get_stormspan_files(params, files)

    for datafile in files:
        storm_timeline[f"{constants.output_dir}/{datafile}"] = {}
        with open(f"{constants.output_dir}/{datafile}") as f:
            storm_data = json.load(f)
            features = storm_data['features'][0]
            datatype = datafile.split("_")[-1].split(".")[0]
            
            if 'advisoryDate' in features['properties']:
                date = 'advisoryDate'
            else:
                date = 'pubAdvTime' 
            storm_timeline[f"{constants.output_dir}/{datafile}"]['UTCdatetime_iso'], storm_timeline['year'] = convert_EDTdate_to_isoformat(params, date)

    # iterate over the first and last files to get the first and latest active days
    # storm_timeline['numdaysactive'] = lastday - firstday

    return storm_timeline 


def get_data_from_url(logger, params, upload, to_download, directory):
    """Given a list of links to download, get data from urls."""

    if upload == 'active':
        if 'tracks' in directory:
            to_download = to_download[-2:] 
        else:
            to_download = [to_download[-1]] 

    for url in to_download:
        status_code = requests.get(url).status_code
        if status_code == 404:
            #print(f'Url status code {status_code}: {url}')
            url = f"https://www.nhc.noaa.gov/gis/archive/{params['main_args']['year']}/{url.split('/')[-1]}"
            #print(f"Reverting to collecting data from {url}")
            status_code = requests.get(url).status_code 
        if status_code == 200:
            try:
                name = url.split("/")[-1].split(".")[0]
                r = requests.get(url)
                z = zipfile.ZipFile(io.BytesIO(r.content))
                z.extractall(f'{directory}/{name}')
            except Exception:
                logger.info(sys.exc_info()[1])
                logger.info(f'Could not retrieve {url}')

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

    logger.info(f"Upload successful to odds.{containerName}: {blobName}")

def reverseGeocode(coordinates): 
    result = rg.search(coordinates) 
    return result

def get_location(datatype, features):
    if datatype in ["WW", "TRACK"]:
        lat = features['geometry']['coordinates'][0][1]
        lon = features['geometry']['coordinates'][0][0]
    elif datatype in ["CONE"]:
        lat = features['geometry']['coordinates'][0][0][1]
        lon = features['geometry']['coordinates'][0][0][0]
    location = reverseGeocode(coordinates=[lat, lon])
    return location

def get_storm_features(params, tropical_storm, storm_datafile, storm_timeline):
    """."""   

    with open(storm_datafile) as f:
        storm_data = json.load(f)
        timeline = storm_timeline[storm_datafile]

    features_dict = {}
    features = storm_data['features'][0]
    datatype = storm_datafile.split("_")[-1].split(".")[0]
    location = get_location(datatype, features)
    features_dict = {
        'state': location[0]['admin1'],
        'county': location[0]['admin2'],
        'storm_name': tropical_storm,
        'storm_region': params[params['main_args']['year']][tropical_storm]['code'][0:2],
        'storm_code': params[params['main_args']['year']][tropical_storm]['code'],
        'fcast_period_h': features['properties']['fcstpd'],
        'storm_type': features['properties']['stormType'],
        'UTCdatetime_iso': timeline['UTCdatetime_iso'],
        'year': params['main_args']['year'],
        'datatype': datatype,
        'first_active_day': '', # storm_timeline['first_active_day'],
        'last_active_day': ''} #storm_timeline['last_active_day']}
    
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
    print(f"Executing query: {q}")
    cursor.execute(q, (guid, features['UTCdatetime_iso'], features['county'], features['state'], features['storm_name'], features['storm_code'], features['storm_type'], features['year'], features['storm_region'], features['datatype'],)) 
    conn.commit()
    conn.close()

