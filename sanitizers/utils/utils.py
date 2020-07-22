from azure.storage.blob import BlobClient
import shutil
import subprocess
from config import constants
import json
import yaml


def get_credentials():
    """.""" 
    with open(constants.creds, 'r') as f:
        creds = dict(yaml.safe_load(f.read()))
            
    return

def remove_crs(path):

    with open(path, 'r') as f:
        data = json.load(f)

    data.pop('crs', None)
    with open(path, 'w') as f:
        json.dump(data, f)

    return 


def cleanup_the_house():
    """Flush data & output dirs."""
    
    shutil.rmtree(f'{constants.alerts_input}')
    shutil.rmtree(f'{constants.alerts_output}')
    shutil.rmtree(f'{constants.activefires_input}')
    shutil.rmtree(f'{constants.activefires_output}')

    return


def convert_to_geojson(logger, inputfile, output_dir):
    """Convert a kml or shapefile to a geojson file, and output in corresponding datadir."""
    
    bash_command = None 
    if "kml" in inputfile:
        bash_command = f"k2g {inputfile} {output_dir}"
    if "shp" in inputfile:
        bash_command = f"ogr2ogr -f GeoJSON {output_dir}/{inputfile.split('/')[-1].split('.')[0]}.geojson {inputfile}"    
        
    logger.info(f'executing file conversion bash command: {bash_command}')
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

    return


def store_blob_in_odds(logger, datafile, token, connectionString, containerName, blobName):
    """Store json files in db."""

    blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=blobName)
    with open(f"{datafile}", 'rb') as f:
        blob.upload_blob(f, overwrite=True)

    logger.info(f"Upload successful to odds.{containerName}: {blobName}")

    return
