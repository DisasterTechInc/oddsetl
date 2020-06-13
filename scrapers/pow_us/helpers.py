from sys import version_info
import os
from config import constants
import urllib.request, json
import urllib, json
from urllib.request import urlopen
from azure.storage.blob import BlobClient

def get_jsonparsed_data(url):
    response = urlopen(url)
    data = response.read().decode("utf-8")
    return json.loads(data)

def get_data(url, county):
  
    out = []
    data = get_jsonparsed_data(url)
    for elem in data:
        for key, value in elem.items():
            if value == county:
                out.append(elem)
   
    if out:
        with open(f"{constants.output_dir}/powout_{county}.geojson", 'w') as f:
            json.dump(out, f)
        return out 

def validate_inputs(params):
    errors = []

    if not version_info[0] > 2:
       errors.append('Python 3+ is required to run pipeline!')

    if type(params["main_args"]['upload']) != bool:
       errors.append('upload argument should be True or False')

    if type(params["main_args"]["county"]) != str:
       errors.append('county argument should be a string')

    if params["main_args"]["odds_container"] not in ["odds", "testcontainer", "po", "demos"]:
       errors.append('odds_container chosen is not allowed, please use "odds", "testcontainer", "po" or "demos" ')

    if errors:
       print(f'Error: {errors}')

    return errors

def store_json_in_odds(params, datafile, token, connectionString, containerName, blobName):
    """Store json files in db."""

    blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=blobName)
    with open(datafile, 'rb') as f:
        blob.upload_blob(f, overwrite=True)

    headers = {"Authorization": "Bearer %s" %token, "content-type":"application/json"}
    print(f"Upload successful to odds.{containerName}: {blobName}")

