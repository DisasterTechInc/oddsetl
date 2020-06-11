from sys import version_info
import shutil
import os
from config import constants
import mysql.connector
import requests, zipfile, io
from bs4 import BeautifulSoup
import subprocess
import uuid
from azure.storage.blob import BlobClient

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
    if params['main_args']['storms_to_get'] != '':
        for storm in [params['main_args']['storms_to_get']]:
            if storm.upper() not in params['all_nhc_storms']:
                erroneous_storms.append(storm)
                errors.append(f"You are requesting data for storms that don't exist in NHC database: {erroneous_storms}!")

    if type(params['main_args']['scrapetype']) != str:
        errors.append("scrapetype should be a string, either 'all' or 'active' ")

    if params['main_args']['scrapetype'] not in ["active","all"]:
        errors.append("scrapetype should be either 'all' or 'active' ")

    if type(params['main_args']['odds_container']) != str:
        errors.append('odds_container should be a string')

    if params['main_args']['odds_container'] not in ["odds", "testcontainer", "nhc", "demos"]:
        errors.append('odds_container chosen is not allowed, please use "odds", "testcontainer", "nhc" or "demos" ')
    if errors:
        print(f'Error: {errors}')

    return errors


def find_files(url):
    soup = BeautifulSoup(requests.get(url).text, features="html.parser")

    links = []
    for element in soup.find_all('a'):
        links.append(element.get_text())
    
    return links


def get_active_storms(url):
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


def get_storms(url):
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


def get_links(url):
    """ Get list of links to download from NHC. """

    list_of_links = find_files(url)
    forecasts = []
    tracks = []
    for link in list_of_links:
        if '.zip' in link:
            forecasts.append(f'https://www.nhc.noaa.gov/gis/forecast/archive/{link}')
        elif '.kmz' in link:
            tracks.append(f'https://www.nhc.noaa.gov/storm_graphics/api/{link}')

    return forecasts, tracks


def convert_to_geojson(params, scrapetype, directory, storm):
    """Convert a kml or shapefile to a geojson file, and output in corresponding datadir."""

    if scrapetype == 'active':
        prefix = 'active_'
    else:
        prefix = ''

    listOfFiles = list()
    for (dirpath, dirnames, filenames) in os.walk(directory):
        listOfFiles += [os.path.join(dirpath, myfile) for myfile in filenames]
    
    listOfFiles = list(set(listOfFiles))

    files = [fi for fi in listOfFiles if fi.endswith(".kml") or fi.endswith(".shp")]
    latest_files = []
    if scrapetype == 'active':
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
                        latest_files.append(myfile)
                elif ".kml" in myfile:
                    if str(maxnums) in str(myfile.split('/')[-1].split("-")[0].split("-")[-1]):
                        latest_files.append(myfile)
  
    if scrapetype =='active':
        files = latest_files

    for myfile in files:
        newfile = None
        if scrapetype == 'active':
            newfile = myfile.split('/')[-1].split("_")[-1]
        else:
            newfile = myfile.split('/')[-1]

        if '.kml' in myfile:
            os.rename(myfile, f"{constants.output_dir}/nhc_{prefix}{storm}_{newfile}")
            myfile = f"{constants.output_dir}/nhc_{prefix}{storm}_{newfile}"
            bashCommand = f"k2g {myfile} {constants.output_dir}"
        elif ".shp" in myfile:
            # myfile.split('/')[-1].split('_')[-1].split('.shp')[0]
            # {constants.output_dir}/{prefix}{storm}_{myfile.split('/')[-1].split('_')[-1].split('.shp')[0]}.geojson"
            filename = f"{constants.output_dir}/nhc_{prefix}{storm}_{newfile}"
            bashCommand = f"ogr2ogr -f GeoJSON {filename} {myfile}"
        try:
            process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()
        except:
            print(f"couldn't convert {myfile}")

    data_list = []
    files = os.listdir(constants.output_dir)
    for myfile in files:
        if ".geojson" in myfile:
            data_list.append(myfile)

    return data_list


def get_data_from_url(upload, to_download, directory):
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
    
    return to_download


def store_json_in_db(datafile, jsonout, token, connectionString, containerName, blobName):
    """Store json files in db."""
    
    blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=blobName)
    with open(datafile, 'rb') as f:
        blob.upload_blob(f, overwrite=True)

    headers = {"Authorization": "Bearer %s" %token, "content-type":"application/json"}
    print(f"Upload successful to odds.{containerName}: {blobName}")


def insert_storms_in_mrt(creds, active_storms):
    """ Insert active storms in Master records table."""
    
    conn = mysql.connector.connect(
            host=creds['azure']['host'],
            user=creds['azure']['user'],
	    password=creds['azure']['password'],
	    db=creds['azure']['db'],
	    port=3306)

    cursor = conn.cursor()
    myid = str(uuid.uuid4())
    ts = str(','.join(active_storms))
    q = f"INSERT INTO master_records_table (guid, active_ts) VALUES(%s, %s);"
    cursor.execute(q, (myid, ts,)) 
    conn.commit()
    conn.close()

