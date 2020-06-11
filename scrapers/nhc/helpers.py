import os
from config import constants
import mysql.connector
import requests, zipfile, io
from bs4 import BeautifulSoup
import subprocess
import uuid
from azure.storage.blob import BlobClient


def find_files(myurl):
    url = myurl
    soup = BeautifulSoup(requests.get(url).text, features="html.parser")

    links = []
    for element in soup.find_all('a'):
        links.append(element.get_text())
    
    return links


def get_active_storms(myurl):
    soup = BeautifulSoup(requests.get(myurl).text, features="html.parser")

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


def get_storms(myurl):
    soup = BeautifulSoup(requests.get(myurl).text, features="html.parser")

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


def get_links(myurl):

    list_of_links = find_files(myurl)
    forecasts = []
    tracks = []
    for link in list_of_links:
        if '.zip' in link:
            forecasts.append(f'https://www.nhc.noaa.gov/gis/forecast/archive/{link}')
        elif '.kmz' in link:
            tracks.append(f'https://www.nhc.noaa.gov/storm_graphics/api/{link}')

    return forecasts, tracks


def convert_to_geojson(params, scrapetype, directory, storm):
   
    if scrapetype == 'latest':
        prefix = 'latest_'
    else:
        prefix = ''

    listOfFiles = list()
    for (dirpath, dirnames, filenames) in os.walk(directory):
        listOfFiles += [os.path.join(dirpath, myfile) for myfile in filenames]
    
    listOfFiles = list(set(listOfFiles))

    files = [fi for fi in listOfFiles if fi.endswith(".kml") or fi.endswith(".shp")]
    latest_files = []
    if scrapetype == 'latest':
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
            # get latest num, sort ascending
            maxnums = max(list(set(nums))) 
            for myfile in files:
                if '.shp' in myfile:
                    if str(maxnums) in str(myfile.split('/')[-1].split("_")[0].split("-")[-1]):
                        latest_files.append(myfile)
                elif ".kml" in myfile:
                    if str(maxnums) in str(myfile.split('/')[-1].split("-")[0].split("-")[-1]):
                        latest_files.append(myfile)
  
    if scrapetype =='latest':
        files = latest_files

    for myfile in files:
        newfile = None
        if scrapetype == 'latest':
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

    if upload == 'latest':
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
    blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=blobName)
    with open(datafile, 'rb') as f:
        blob.upload_blob(f, overwrite=True)

    headers = {"Authorization": "Bearer %s" %token, "content-type":"application/json"}
    #response = requests.put(f"https://odds.disastertech.com/", data=str(jsonout).replace("\'","\""), headers=headers, verify=False)
    print(f"Upload successful to odds.{containerName}: {blobName}")


def insert_storms_in_mrt(creds, active_storms):
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

